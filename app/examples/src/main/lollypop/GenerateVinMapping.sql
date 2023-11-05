namespace `demo.aws`
//////////////////////////////////////////////////////////////////////////////////////
//      GENERATE VIN-MAPPING DEMO
// include('./app/examples/src/main/lollypop/GenerateVinMapping.sql')
//////////////////////////////////////////////////////////////////////////////////////

def main(args: String[]) := {
    import ["java.text.SimpleDateFormat", "java.io.BufferedWriter", "java.io.FileWriter"]
    items = [
        { masked: '73cf1590-5509-4606-a694-0fd08e9b547a', vin: 'ABCD3XYZXKD004606' },
        { masked: '4df3c7a4-1875-4fcf-8610-9c1d41a92ed5', vin: 'ABCD3XYZ2LD058610' },
        { masked: '297a57b0-7fb7-4320-9c30-65c6a7e5f437', vin: 'ABCD3XYZ7LJ024320' },
        { masked: 'a2d476d3-7c02-49ba-9aba-493a89852ba7', vin: 'ABCD3XYZ6KD508985' },
        { masked: 'eb961abf-524f-4597-97a0-9f14e9e5d24f', vin: 'ABCD3XYZ7LD524597' }
    ].toTable()

    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    with new BufferedWriter(new FileWriter('./vin-mapping.json')) out =>
        each item in @items {
            createTime = sdf.format(DateTime())
            payload = {'Item':{
                'CreatedTime':{'S': createTime},
                'MaskedVin':{'S': masked},
                'Vin':{'S': vin},
                'aws:rep:deleting':{'BOOL':false},
                'aws:rep:updateregion':{'S':'us-east-1'},
                'aws:rep:updatetime':{'N':'1590600503.813001'}}}
            out.write(payload.toJsonString())
            out.newLine()
        }
    OS.read('./vin-mapping.json')
}

main([])

