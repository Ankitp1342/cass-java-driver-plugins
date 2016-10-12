# cass-java-driver-plugins


This project contains code to implement a ExponentialRetryPolicy in datastax java driver (https://github.com/datastax/java-driver). This retry policy will allow users to retry by exponentially backing off with ceiling of max retry delay with unlimited retry. You can optionally set the max number of retries to throw error after the max retries have reached.
