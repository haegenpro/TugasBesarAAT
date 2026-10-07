FROM apicurio/apicurio-registry:3.3.3
COPY --chmod=755 entrypoint.sh /registry-entrypoint.sh
ENTRYPOINT ["/registry-entrypoint.sh"]
CMD ["/opt/jboss/container/java/run/run-java.sh"]
