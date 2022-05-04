locals {
  bucket              = "${var.project}-${var.owner}-bucket-with-startup-script"
  startup-script-name = "startup-script.sh"
}

data "google_compute_network" "network" {
  project = var.project
  name    = var.network
}

data "google_compute_subnetwork" "subnetwork" {
  project = var.project
  region  = var.region
  name    = var.subnetwork
}

// gcloud compute addresses create kafka-ip --region $REGION --subnet $SUBNETWORK --addresses 10.128.0.77
data "google_compute_address" "kafka_ip_address" {
  project = var.project
  region  = var.region
  name    = "${var.owner}-kafka-ip-address"
}

output "kafka-ip" {
  value = data.google_compute_address.kafka_ip_address.address
}

//resource "google_compute_address" "kafka-address" {
//  name         = "kafka-address"
//  project      = var.project
//  region       = var.region
//  address_type = "INTERNAL"
//  purpose      = "GCE_ENDPOINT"
//}

resource "google_compute_instance" "kafka_jmeter_vm" {
  name         = "${var.owner}-kafka-jmeter-vm"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-medium"
  metadata = {
    "enable-oslogin" = "TRUE"
  }

  boot_disk {
    initialize_params {
      image = var.image
      size = 256
    }
  }

  network_interface {
    network_ip = data.google_compute_address.kafka_ip_address.address
    network    = data.google_compute_network.network.id
    subnetwork = data.google_compute_subnetwork.subnetwork.id
  }

  metadata_startup_script = <<-EOF
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(1/6) Started startup script ..."
    df -h
    sudo lvextend -r /dev/vg00/lv_root /dev/sda2
    df -h
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(2/6) Disc resized ..."

    mkdir /apps
    chmod 777 /apps
    ls -ls /apps
    gsutil cp gs://${var.project}-${var.owner}/kafka_2.11-1.1.1.tgz /apps/
    cd /apps
    tar xzf kafka_2.11-1.1.1.tgz
    cd kafka_2.11-1.1.1
    sed -i 's/dataDir=\/tmp\/zookeeper/dataDir=\/apps\/zookeeper/' config/zookeeper.properties
    cat config/zookeeper.properties
    sed -i 's/log.dirs=\/tmp\/kafka-logs/log.dirs=\/apps\/kafka-logs/' config/server.properties

    echo "" >> config/server.properties
    echo "### extend max message size to 10MB ###" >> config/server.properties
    echo "message.max.bytes=10485760" >> config/server.properties
    nohup bash -c "./bin/zookeeper-server-start.sh config/zookeeper.properties" > zookeeper.log 2>&1&
    sleep 5
    cat zookeeper.log
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(3/6) Started zookeeper ..."

    nohup bash -c "./bin/kafka-server-start.sh config/server.properties" > kafka.log 2>&1&
    sleep 5
    cat kafka.log
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(4/6) Started kafka ..."

    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic my-topic --config retention.ms=3600000
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(5/6) Created topic my-topic ..."

    cd /apps/
    gsutil cp gs://${var.project}-${var.owner}/apache-jmeter-5.4.1.tgz /apps/
    tar xzf apache-jmeter-5.4.1.tgz
    cd apache-jmeter-5.4.1

    cd /apps/
    mkdir jmeter
    chmod 777 jmeter
    gsutil cp gs://${var.project}-${var.owner}/jmeter/* /apps/jmeter/

    cd /apps/jmeter
    /apps/apache-jmeter-5.4.1/bin/jmeter.sh -Duser.classpath=/apps/jmeter/ -n -t kafka-jmeter-plugin-tps.jmx -Jbootstrap.servers=localhost:9092 -Jtopic=my-topic -JavroFlumeEnabled=true -JkafkaHeadersJson='{"k_k1":"k_v1","k_k2":"k_v2"}' -JavroFlumeHeadersJson='{"af_k":"af_v"} -JtestThreads=1 -JtestTps=16 -JtestDurationSec=10  '
    sleep 5
    cat jmeter.log
    chmod 666 jmeter.log
    gcloud compute instances add-metadata --zone ${var.zone} ${var.owner}-vm --metadata=startup-state="(6/6) Run jmeter test ..."
  EOF

  service_account {
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      pattern="Successfully sent message"
      max_retry=3
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.kafka_jmeter_vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep "$pattern"
      do sleep 120
      if [[ counter -eq $max_retry ]]
      then
        echo "Startup script '$pattern' pattern not found in ${google_compute_instance.kafka_jmeter_vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.kafka_jmeter_vm.name} logs to contain startup script '$pattern' pattern (attempt: $counter)"
        ((counter++))
      fi
      done
    EOT
  }
}
