FROM confluentinc/cp-kafka-connect:8.3.2

# Snowflake sink (Snowpipe Streaming) + S3 sink pointed at MinIO for the
# archive/replay zone. Pinned versions so builds are reproducible.
RUN confluent-hub install --no-prompt snowflakeinc/snowflake-kafka-connector:2.2.2 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.13
