output "vm_instance_ip" {
  value = google_compute_instance.vm-terraform[0].network_interface[0].access_config[0].nat_ip
}
