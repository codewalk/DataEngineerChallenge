# Building the JAR

    mvn clean package
* Above command will run test cases regarding reading the file and sessionizing th data
* Fat jar will be created under target folder which will be used for running the spark application
    
    
    java -cp target\DataChallenge-1.0-SNAPSHOT-jar-with-dependencies.jar com.paypay.Application -Doutput_data_path=D:\projects\DataEngineerChallenge\output -Dinput_data_path=D:\projects\DataEngineerChallenge\data\2015_07_22_mktplace_shop_web_log_sample.log.gz -DENV=LOCAL
    
* DataChallenge-1.0-SNAPSHOT-jar-with-dependencies.jar => Jar File
* com.paypay.Application => Main class
* output_data_path => Folder to generate the output files
* input_data_path => Path for the data file.

If input_data_path is not provided, then data file will be read from project location itself i.e. data\2015_07_22_mktplace_shop_web_log_sample.log.gz

If output_data_path is not provided then o/p data files will be generated under Project location iteself i.e output\ 
