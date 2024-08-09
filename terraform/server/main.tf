resource "google_compute_address" "static" {
  name = "static-ip"
}

resource "google_compute_instance" "vm-terraform" {
  count        = 1
  name         = "vm-spark-arruda-${count.index}"
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-2204-jammy-v20240112"
      size  = var.disk_size
    }
  }

  network_interface {
    network = "default"

    // Condição para atribuir o IP estático apenas à primeira máquina
    access_config {
      nat_ip = count.index == 0 ? google_compute_address.static.address : null
    }
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -xe

    # Install Docker
    curl -fsSL https://get.docker.com -o get-docker.sh
    cd ..
    cd ..
    sh get-docker.sh
    
  EOT
}
