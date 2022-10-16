FROM amazon/aws-cli

COPY . /app/

RUN chmod +x /app/s3-provision.sh

ENTRYPOINT ["sh", "-c", "/app/s3-provision.sh"]
