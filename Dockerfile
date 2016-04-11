FROM java

RUN mkdir /moquette
COPY distribution/target/distribution-0.9-SNAPSHOT-bundle-tar.tar.gz /moquette
WORKDIR moquette
RUN tar -xvf distribution-0.9-SNAPSHOT-bundle-tar.tar.gz
WORKDIR bin

CMD [ "/moquette/bin/moquette.sh" ]
