package com.github.ldaniels528.lollypop;

import java.io.Serializable;

public class FakeModel implements Serializable {
    public final int value;

    public FakeModel(int value) {
        this.value = value;
    }

}
