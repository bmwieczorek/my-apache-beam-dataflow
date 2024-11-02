package com.bawi.beam.dataflow.geecon;

public class XmlPayload {
    public static final String XML_PAYLOAD =
            """
                <college id="1">
                    <staff id="101" dep_name="Admin">
                        <employee id="101-01" name="ashish"/>
                        <employee id="101-02" name="amit"/>
                        <employee id="101-03" name="nupur"/>
                        <salary id="101-sal">
                            <basic>20000</basic>
                            <special-allowance>50000</special-allowance>
                            <medical>10000</medical>
                            <provident-fund>10000</provident-fund>
                        </salary>
                    </staff>
                    <staff id="102" dep_name="HR">
                        <employee id="102-01" name="shikhar"/>
                        <employee id="102-02" name="sanjay"/>
                        <employee id="102-03" name="ani"/>
                        <salary id="102-sal">
                            <basic>25000</basic>
                            <special-allowance>60000</special-allowance>
                            <medical>10000</medical>
                            <provident-fund>12000</provident-fund>
                        </salary>
                    </staff>
                    <staff id="103" dep_name="IT">
                        <employee id="103-01" name="suman"/>
                        <employee id="103-02" name="salil"/>
                        <employee id="103-03" name="amar"/>
                        <salary id="103-sal">
                            <basic>35000</basic>
                            <special-allowance>70000</special-allowance>
                            <medical>12000</medical>
                            <provident-fund>15000</provident-fund>
                        </salary>
                    </staff>
                </college>
            """;
}
