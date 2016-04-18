FROM java:openjdk-8-jdk

# config, mainjar, logs and data folders respectively
RUN mkdir /etc/sonicd/; mkdir /usr/local/sonicd; mkdir /var/log/sonicd; mkdir /var/sonicd
RUN chmod 0775 /etc/sonicd; chmod 0775 /usr/local/sonicd; chmod 0775 /var/log/sonicd/; chmod 0775 /var/sonicd

ENV configfolder /etc/sonicd/
ENV lib /var/lib/sonicd/*
ENV mainclass build.unstable.sonicd.Sonic
ENV mainjar /usr/local/sonicd/sonicd-assembly.jar
ENV cp $configfolder:$mainjar:$lib
#ENV javaoptions=''

ADD server/target/scala-2.11/sonicd-assembly.jar /usr/local/sonicd/

CMD java -cp $cp $mainclass
