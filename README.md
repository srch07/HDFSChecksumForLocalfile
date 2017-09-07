# HDFSChecksumForLocalfile
This program / jar creates checksum, with same algorithm that hadoop uses to create on hdfs files. So integrity of file can be verified on local and hadoop system. Can also, be used to check if file exist based on checksum, before uploading and cluttering hdfs with duplicate files.

## How to test it?

Download the jar from the archive directory.

It can be fired using java -jar "jar_name" "arguments"

Arguments can be provided in either of the below format:

1. file name with path

    eg, java -jar "this_jar_filename_with_path" "local file path"

2. filename_with_path BytesPerChecksum ChecksumPerBlock

    eg, java -jar "this_jar_filename_with_path" "local file path" 256 512

3. filename_with_path BytesPerChecksum ChecksumPerBlock AlgorithmType(CRC32/CRC32C/NULL/DEFAULT/MIXED)

    eg, java -jar "this_jar_filename_with_path" "local file path" 256 512 CRC32C

## How to use this jar in your project?

Add this jar to your class path and import com.srch07.HadoopChecksum

use HadoopChecksum.calculate(filepath) or it's overloaded multiple signatures just like command line.
It will return a String value of checksum.

Note : This project uses hadoop-common maven library internally. So for whatever purpose you are using already a version of it in your project, you can define the scope in this project as provided.
