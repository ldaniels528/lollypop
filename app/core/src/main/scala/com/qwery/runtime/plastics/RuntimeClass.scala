package com.qwery.runtime.plastics

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models._
import com.qwery.language.{LanguageParser, QweryUniverse}
import com.qwery.runtime.datatypes.DataTypeParser
import com.qwery.runtime.instructions.ScalaConversion
import com.qwery.runtime.instructions.expressions.NamedFunctionCall
import com.qwery.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.{QweryException, die}
import dev.jeka.core.api.depmanagement.resolution.JkDependencyResolver
import dev.jeka.core.api.depmanagement.{JkDependencySet, JkRepo, JkRepoSet}
import org.slf4j.LoggerFactory
import qwery.io.IOCost

import java.io.File
import java.lang.reflect.{Constructor, Executable, Field, Method}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.ListHasAsScala

object RuntimeClass {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val classes = TrieMap[Class[_], ClassComponents]()
  private var virtualMethods: List[VirtualMethod] = Nil

  def apply(`class`: Class[_]): ClassComponents = {
    // capture the constructor, fields and methods
    classes.getOrElseUpdate(`class`,
      ClassComponents(
        `class` = `class`,
        constructors = `class`.getDeclaredConstructors,
        fields = Map(`class`.getCompleteFieldList.map(f => f.getName -> f): _*),
        methods = `class`.getMethods.groupBy(_.getName)))
  }

  def decodeModifiers(modifiers: Int): List[String] = {
    import java.lang.reflect.Modifier
    val mapping = List(
      Modifier.VOLATILE -> "volatile",
      Modifier.NATIVE -> "native",
      Modifier.STRICT -> "strict",
      Modifier.PRIVATE -> "private",
      Modifier.PUBLIC -> "public",
      Modifier.PROTECTED -> "protected",
      Modifier.STATIC -> "static",
      Modifier.FINAL -> "final",
      Modifier.ABSTRACT -> "abstract",
      Modifier.INTERFACE -> "interface",
      Modifier.SYNCHRONIZED -> "synchronized",
      Modifier.TRANSIENT -> "transient"
    )
    mapping.collect { case (code, modifier) if code.&(modifiers) != 0 => modifier }
  }

  def downloadDependencies(dependencies: String*): List[File] = {
    val dependencySet = dependencies.foldLeft(JkDependencySet.of()) { (agg, dep) => agg.and(dep) }
    val result = JkDependencyResolver.of()
      .setRepos(JkRepoSet.of(JkRepo.ofMavenCentral()))
      .resolve(dependencySet)
    result.getFiles.getEntries.asScala.toList.map(_.toFile)
  }

  def getClassByName(className: String)(implicit scope: Scope): Class[_] = {
    val actualClassName = scope.getImports.get(className) || className
    apply(Class.forName(actualClassName, true, scope.getUniverse.classLoader)).`class`
  }

  def getCommonAncestor(a: Class[_], b: Class[_]): Class[_] = {
    ((getSuperClasses(a) intersect getSuperClasses(b)) ::: (getInterfaces(a) intersect getInterfaces(b)))
      .filterNot(_ == classOf[Object])
      .headOption || classOf[Object]
  }

  def getInterfaces(_class: Class[_]): List[Class[_]] = {
    _class.getInterfaces match {
      case null => Nil
      case array =>
        val list = array.toList
        (list ::: list.flatMap(getInterfaces) ::: Option(_class.getSuperclass).toList.flatMap(getInterfaces)).distinct
    }
  }

  def getObjectByName(className: String)(implicit classLoader: ClassLoader): Any = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(classLoader)
    val module = runtimeMirror.staticModule(className)
    runtimeMirror.reflectModule(module).instance
  }

  def getSuperClasses(_class: Class[_]): List[Class[_]] = {
    _class.getSuperclass match {
      case null => Nil
      case c => c :: getSuperClasses(c).distinct
    }
  }

  def getSuperClassesAndInterfaces(_class: Class[_]): List[Class[_]] = {
    getSuperClasses(_class) ::: getInterfaces(_class)
  }

  def getVirtualMethods(instance: Any): List[VirtualMethod] = {
    virtualMethods.collect { case vm if vm.isMatch(instance) => vm }
  }

  /**
   * Invokes a virtual method of a JVM class/object instance
   * @param instance the JVM object instance
   * @param name     the name of virtual method to invoke
   * @param params   the virtual method arguments
   * @return the result of the virtual method invocation
   */
  def invokeVirtualMethod(instance: Any, name: String, params: Seq[Expression])(implicit scope: Scope): Option[Any] = {
    val `class` = instance.getClass
    val vmCandidates = virtualMethods.filter(vm => vm.isMatch(instance) | vm.isMatch(`class`))
    vmCandidates.find(vm => vm.method.name == name && vm.method.params.length == params.length + 1) map { _ =>
      val vmArgs = instance.v :: params.toList
      val vmScope = vmCandidates.foldLeft(scope) { case (aggScope, vmc) =>
        aggScope.withVariable(vmc.method.name, value = vmc.method, isReadOnly = true)
      }
      QweryVM.execute(vmScope, NamedFunctionCall(name, args = vmArgs))._3
    }
  }

  def loadExternalComponent(extComp: ExternalComponent)(implicit scope: Scope): (Scope, IOCost, ExternalComponent) = {
    logger.debug(s"Analyzing component '${extComp.`class`}'...")
    val module = RuntimeClass.getObjectByName(extComp.`class`)(scope.getUniverse.classLoader)
    var universe: QweryUniverse = scope.getUniverse

    // is it a dataType parser?
    module match {
      case dtp: DataTypeParser =>
        logger.debug(s"Adding data type parser from component '${extComp.`class`}'...")
        universe = universe.copy(dataTypeParsers = dtp :: universe.dataTypeParsers)
      case _ =>
    }

    // is it a language parser?
    module match {
      case lp: LanguageParser =>
        logger.debug(s"Adding language parser from component '${extComp.`class`}'...")
        universe = universe.copy(languageParsers = lp :: universe.languageParsers)
      case _ =>
    }

    (scope.withEnvironment(universe), IOCost.empty, extComp)
  }

  def loadJarFiles(jarFiles: Seq[File])(implicit scope: Scope): Unit = {
    val urls = jarFiles.map(_.toURI.toURL)
    val cl = scope.getUniverse.classLoader
    urls.foreach(cl.addURL)
  }

  def registerVirtualMethod(virtualMethod: VirtualMethod): Unit = {
    virtualMethods = virtualMethod :: virtualMethods
  }

  def resolveClass(classes: Seq[Class[_]], isNullable: Boolean): Class[_] = {
    val _classes = classes.distinct
    val _class = _classes match {
      case Nil => if (isNullable) classOf[AnyRef] else classOf[Any]
      case List(candidate) => candidate
      case candidates => candidates.reduce(getCommonAncestor)
    }
    updatePrimitives(_class, isNullable)
  }

  private def updatePrimitives(_class: Class[_], isNullable: Boolean): Class[_] = {
    _class match {
      // Boolean
      case c if c == classOf[Boolean] & isNullable => classOf[java.lang.Boolean]
      case c if c == classOf[java.lang.Boolean] & !isNullable => classOf[Boolean]
      // Byte
      case c if c == classOf[Byte] & isNullable => classOf[java.lang.Byte]
      case c if c == classOf[java.lang.Byte] & !isNullable => classOf[Byte]
      // Char
      case c if c == classOf[Char] & isNullable => classOf[java.lang.Character]
      case c if c == classOf[java.lang.Character] & !isNullable => classOf[Char]
      // Double
      case c if c == classOf[Double] & isNullable => classOf[java.lang.Double]
      case c if c == classOf[java.lang.Double] & !isNullable => classOf[Double]
      // Float
      case c if c == classOf[Float] & isNullable => classOf[java.lang.Float]
      case c if c == classOf[java.lang.Float] & !isNullable => classOf[Float]
      // Int
      case c if c == classOf[Int] & isNullable => classOf[java.lang.Integer]
      case c if c == classOf[java.lang.Integer] & !isNullable => classOf[Int]
      // Long
      case c if c == classOf[Long] & isNullable => classOf[java.lang.Long]
      case c if c == classOf[java.lang.Long] & !isNullable => classOf[Long]
      // Short
      case c if c == classOf[Short] & isNullable => classOf[java.lang.Short]
      case c if c == classOf[java.lang.Short] & !isNullable => classOf[Short]
      case c => c
    }
  }

  private def convert(expression: Expression)(implicit scope: Scope): Any = {
    evaluate(expression) match {
      case sc: ScalaConversion => sc.toScala
      case xx => xx
    }
  }

  private def evaluate(expression: Expression)(implicit scope: Scope): Any = {
    QweryVM.execute(scope, expression)._3
  }

  private def getComponents(instance: Any): ClassComponents = {
    instance match {
      case _class: Class[_] => apply(_class)
      case v => apply(v.getClass)
    }
  }

  case class ClassComponents(`class`: Class[_],
                             constructors: Seq[Constructor[_]],
                             fields: Map[String, Field],
                             methods: Map[String, Array[Method]]) {

    def containsField(name: String): Boolean = fields.contains(name)

    def containsMethod(name: String, params: Seq[Any]): Boolean = methods.get(name).flatMap(findCandidate(_, params)).nonEmpty

    /**
     * Retrieves a field of a JVM class/object instance by name
     * @param name the name of field to retrieve
     * @return an option of the field
     */
    @inline
    def findField(name: String): Option[Field] = fields.get(name)

    /**
     * Retrieves a method of a JVM class/object instance
     * @param name   the name of method to invoke
     * @param params the method arguments
     * @return the result of the method invocation
     */
    def findMethod(name: String, params: Seq[Any]): Option[Method] = methods.get(name).flatMap(findCandidate(_, params))

    def instantiate(params: Seq[Any]): Any = {
      findCandidate(constructors, params).map(_.newInstance(params: _*)) ||
        die(s"Constructor '${`class`.getSimpleName}${params.map(_.getClass.getName).mkString("(", ", ", ")")}' in class '${`class`.getName}' was not found")
    }

    /**
     * Evaluates a field of a JVM class/object instance
     * @param instance the JVM object instance
     * @param name     the name of field to invoke
     * @return the result of the field invocation
     */
    def invokeField(instance: Any, name: String): AnyRef = {
      val field = fields.getOrElse(name, die(s"Field '$name' in class '${`class`.getName}' was not found"))
      field.get(instance)
    }

    /**
     * Invokes a method of a JVM class/object instance
     * @param instance the JVM object instance
     * @param name     the name of method to invoke
     * @param params   the method arguments
     * @return the result of the method invocation
     */
    def invokeMethod(instance: Any, name: String, params: Seq[Expression])(implicit scope: Scope): AnyRef = {
      val args = params.map(convert)
      try {
        // it it an implicit class method?
        if (scope.isImplicitMethod(name, args: _*)) scope.invokeImplicitMethod(instance, name, args: _*)
        else {
          // is it a JVM method?
          methods.get(name).flatMap(findCandidate(_, args)).map(_.invoke(instance, args: _*)) ??
            // is it a virtual method?
            invokeVirtualMethod(instance, name, params) ||
            // not found
            failedMethod(name, args, QweryException("was not found"))
        }
      } catch {
        case e: IllegalAccessException => failedMethod(name, args, e)
        case e: IllegalArgumentException => failedMethod(name, args, e)
        case e: java.lang.reflect.InvocationTargetException => failedMethod(name, args, e)
      }
    }

    private def toErrorMessage(e: Throwable): String = {
      Option(e).flatMap(x => Option(x.getMessage)) ?? Option(e.getCause).flatMap(x => Option(x.getMessage)) || e.getClass.getSimpleName
    }

    private def failedMethod(name: String, params: Seq[Any], e: Throwable): Nothing = {
      die(s"Method '$name${params.map(_.getClass.getName).mkString("(", ", ", ")")}' in class '${`class`.getName}': ${toErrorMessage(e)}", e)
    }

    @tailrec
    private def findCandidate[A <: Executable](candidates: Seq[A], params: Seq[Any], pass: Int = 0): Option[A] = {
      candidates match {
        // is there just a single candidate?
        case Seq(candidate) => Option(candidate)
        case _ =>
          pass match {
            case 0 => findCandidate(candidates.filter(_.getParameterCount == params.size), params, pass = pass + 1)
            case 1 => findCandidate(candidates.filter(c => isCompatible(c.getParameterTypes, params)), params, pass = pass + 1)
            case 2 => candidates.headOption
          }
      }
    }

    private def isCompatible(parameterTypes: Seq[Class[_]], params: Seq[Any]): Boolean = {
      params.zip(params.map(_.getClass)) zip parameterTypes forall {
        case ((pv, pvc), ptc) if (pv == null) | (pvc == ptc) => true
        case ((_, pvc), ptc) if pvc.isDescendantOf(ptc) || ptc.isDescendantOf(pvc) => true
        case ((pv, _), ptc) if ptc.isPrimitive & isBoxed(pv, ptc) => true
        case _ => false
      }
    }

    private def isBoxed(pv: Any, ptc: Class[_]): Boolean = pv match {
      case pv if pv.isInstanceOf[java.lang.Boolean] & (ptc == classOf[Boolean]) => true
      case pv if pv.isInstanceOf[java.lang.Byte] & (ptc == classOf[Byte]) => true
      case pv if pv.isInstanceOf[java.lang.Character] & (ptc == classOf[Char]) => true
      case pv if pv.isInstanceOf[java.lang.Double] & (ptc == classOf[Double]) => true
      case pv if pv.isInstanceOf[java.lang.Float] & (ptc == classOf[Float]) => true
      case pv if pv.isInstanceOf[java.lang.Integer] & (ptc == classOf[Int]) => true
      case pv if pv.isInstanceOf[java.lang.Long] & (ptc == classOf[Long]) => true
      case pv if pv.isInstanceOf[java.lang.Short] & (ptc == classOf[Short]) => true
      case _ => false
    }

  }

  object implicits {

    final implicit class RuntimeClassProduct(val product: Product) extends AnyVal {
      @inline def productElementFields: List[Field] = {
        val names = product.productElementNames.toSet
        product.getClass.getDeclaredFields.toList.filter(f => names.contains(f.getName))
      }
    }

    /**
     * RuntimeClass Constructor Sugar
     * @param class the host [[Class class]]
     */
    final implicit class RuntimeClassConstructorSugar(val `class`: Class[_]) extends AnyVal {

      @inline
      def getCompleteFieldList: List[Field] = {
        (`class`.getDeclaredFields ++ `class`.getFields).distinct
          .filterNot(f => f.getName.startsWith("__$") && f.getName.endsWith("$__")).toList
      }

      @inline
      def getFilteredDeclaredFields: List[Field] = {
        `class`.getDeclaredFields.toList.filterNot(f => f.getName.startsWith("__$") && f.getName.endsWith("$__"))
      }

      @inline
      def getJavaTypeName: String = {
        `class` match {
          case c if c.isArray => c.getComponentType.getJavaTypeName + "[]"
          case c => c.getSimpleName
        }
      }

      @inline
      def instantiate(params: Any*): Any = getComponents(`class`).instantiate(params)

      @inline
      def isDescendantOf(ancestor: Class[_]): Boolean = {
        (ancestor != null && `class` != null) && (
          `class` == ancestor ||
            Option(`class`).flatMap(d => Option(d.getSuperclass)).exists(_.isDescendantOf(ancestor)) |
            `class`.getInterfaces.exists(_.isDescendantOf(ancestor)))
      }

    }

    /**
     * RuntimeClass Constructor Sugar
     * @param className the class name
     */
    final implicit class RuntimeClassNameAtomConstructorSugar(val className: Atom) extends AnyVal {
      @inline
      def instantiate(args: Expression*)(implicit scope: Scope): Any = {
        getClassByName(className.name).instantiate(args.map(QweryVM.execute(scope, _)._3): _*)
      }
    }

    /**
     * RuntimeClass Constructor Sugar
     * @param className the class name
     */
    final implicit class RuntimeClassNameConstructorSugar(val className: String) extends AnyVal {
      @inline
      def instantiate(args: Expression*)(implicit scope: Scope): Any = {
        getClassByName(className).instantiate(args.map(QweryVM.execute(scope, _)._3): _*)
      }
    }

    /**
     * RuntimeClass Expression Sugar
     * @param expression the host [[Expression expression]]
     */
    final implicit class RuntimeClassExpressionSugar(val expression: Expression) extends AnyVal {

      @inline
      def containsMember(member: Expression)(implicit scope: Scope): Boolean = {
        Option(evaluate(expression)).map(_.asInstanceOf[AnyRef]) exists { instance =>
          member match {
            case NamedFunctionCall(name, args) =>
              instance.containsMethod(name, params = args.map(QweryVM.execute(scope, _)._3))
            case FieldRef(name) => instance.containsField(name)
            case other => expression.dieIllegalType(other)
          }
        }
      }

      @inline
      def invokeMember(member: Expression)(implicit scope: Scope): AnyRef = {
        (Option(evaluate(expression)).map(_.asInstanceOf[AnyRef]) map { instance =>
          member match {
            case NamedFunctionCall(name, params) => instance.invokeMethod(name, params)
            case FieldRef(name) => instance.invokeField(name)
            case other => expression.dieIllegalType(other)
          }
        }).orNull
      }
    }

    /**
     * RuntimeClass Instance Sugar
     * @param instance the host [[Any instance]]
     */
    final implicit class RuntimeClassInstanceSugar(val instance: Any) extends AnyVal {

      @inline
      def containsField(name: String): Boolean = getComponents(instance).containsField(name)

      @inline
      def containsMethod(name: String, params: Seq[Any] = Nil): Boolean = getComponents(instance).containsMethod(name, params)

      /**
       * Retrieves a field of a JVM class/object instance by name
       * @param name the name of field to retrieve
       * @return an option of the field
       */
      @inline
      def findField(name: String): Option[Field] = getComponents(instance).findField(name)

      /**
       * Retrieves a method of a JVM class/object instance by name
       * @param name   the name of method to retrieve
       * @param params the method arguments
       * @return an option of the method
       */
      @inline
      def findMethod(name: String, params: Seq[Any]): Option[Method] = getComponents(instance).findMethod(name, params)

      /**
       * Evaluates a field of a JVM class/object instance
       * @param name the name of field to invoke
       * @return the result of the field invocation
       */
      @inline
      def invokeField(name: String): AnyRef = getComponents(instance).invokeField(instance, name)

      /**
       * Invokes a method of a JVM class/object instance
       * @param name   the name of method to invoke
       * @param params the method arguments
       * @return the result of the method invocation
       */
      @inline
      def invokeMethod(name: String, params: Seq[Expression] = Nil)(implicit scope: Scope): AnyRef = {
        getComponents(instance).invokeMethod(instance, name, params)
      }

      /**
       * Invokes a virtual method of a JVM class/object instance
       * @param name   the name of method to invoke
       * @param params the method arguments
       * @return the result of the method invocation
       */
      @inline
      def invokeVirtualMethod(name: String, params: Seq[Expression] = Nil)(implicit scope: Scope): AnyRef = {
        RuntimeClass.invokeVirtualMethod(instance, name, params)
      }

    }

  }

}