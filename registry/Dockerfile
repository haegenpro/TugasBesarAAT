# Keep the release's Java application, but use UBI 9: the upstream UBI 10
# runtime requires x86-64-v3, which some VirtualBox guests do not expose.
FROM apicurio/apicurio-registry@sha256:c9cae4c90ce46538abf673c68eb591345f23bcc6c2aa6bfd47189adcf609d8f1 AS distribution
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime@sha256:9a074c2ff90be60b12521ffd5e531acafd25fe36f1c177180cbe1176dfb36c84
COPY --from=distribution --chown=185:0 /deployments/ /deployments/
# Reserve room for metaspace, code cache, stacks and native allocations inside
# M2's 256 MiB limit; the runtime's default 80% Java heap leaves too little.
ENV GC_CONTAINER_OPTIONS="-XX:+UseSerialGC" \
    JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Xms24m -Xmx64m -XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=96m -XX:ReservedCodeCacheSize=16m -XX:MaxDirectMemorySize=16m -Xss256k"
COPY --chmod=755 entrypoint.sh /registry-entrypoint.sh
ENTRYPOINT ["/registry-entrypoint.sh"]
CMD ["/opt/jboss/container/java/run/run-java.sh"]
