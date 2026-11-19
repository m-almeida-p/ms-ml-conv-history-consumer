# The DevMode image Karavan runs this project in, with ONE change: where it fetches the project's files from.
#
# The stock image's entrypoint hard-codes PLATFORM_HOST="karavan:8080" (the Karavan server, by its Docker DNS name). Another Karavan
# stack on the same network (ms-ml-camel-conv-history-bs) is also called `karavan`, so the container of this stack would ask THAT
# Karavan for its files, get an HTTP 500 and start with no routes ("Routes startup (total:0)"). This stack's Karavan is called
# consumer-karavan, so the image is told to ask it.
FROM ghcr.io/apache/camel-karavan-devmode:4.22.2
RUN sed -i 's#PLATFORM_HOST="karavan:8080"#PLATFORM_HOST="consumer-karavan:8080"#' /karavan/entrypoint.sh \
 && grep -q 'PLATFORM_HOST="consumer-karavan:8080"' /karavan/entrypoint.sh
