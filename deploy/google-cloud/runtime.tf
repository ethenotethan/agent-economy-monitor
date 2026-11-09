locals {
  worker_modes      = toset(["collect", "verify-evidence", "reduce", "classify", "enrich"])
  worker_secret_ids = var.worker_secret_ids
  worker_plain_environment = {
    collect         = { COLLECTION_INPUT_ROOT = "/collection-inputs", EVIDENCE_WRITE_BUCKET = google_storage_bucket.evidence.name }
    verify-evidence = { EVIDENCE_READ_BUCKET = google_storage_bucket.evidence.name }
    reduce          = {}
    classify        = {}
    enrich          = { EVIDENCE_WRITE_BUCKET = google_storage_bucket.evidence.name }
  }
  worker_volume_names = {
    collect         = ["cloudsql", "collection-inputs"]
    verify-evidence = ["cloudsql"]
    reduce          = ["cloudsql"]
    classify        = ["cloudsql"]
    enrich          = ["cloudsql"]
  }
}

resource "google_cloud_run_v2_service" "cockpit" {
  name                 = "agent-economy-monitor"
  location             = var.region
  deletion_protection  = true
  ingress              = "INGRESS_TRAFFIC_ALL"
  iap_enabled          = false
  invoker_iam_disabled = true
  depends_on           = [google_project_service.required]

  template {
    service_account = google_service_account.runtime.email
    timeout         = "300s"

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.postgres.connection_name]
      }
    }

    containers {
      image = var.image
      args  = ["serve"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "NAMESPACE_ID"
        value = var.namespace_id
      }

      dynamic "env" {
        for_each = var.service_secret_ids
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value
              version = env.key == "COCKPIT_PASSWORD_HASH" ? tostring(var.cockpit_password_secret_version) : "latest"
            }
          }
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      startup_probe {
        initial_delay_seconds = 1
        timeout_seconds       = 2
        period_seconds        = 2
        failure_threshold     = 15
        http_get {
          path = "/healthz"
        }
      }

      liveness_probe {
        timeout_seconds   = 2
        period_seconds    = 30
        failure_threshold = 3
        http_get {
          path = "/healthz"
        }
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_cloud_run_v2_job" "worker" {
  for_each = toset(["collect", "verify-evidence", "reduce", "classify", "enrich"])

  name                = "agent-economy-${each.key}"
  location            = var.region
  deletion_protection = true
  depends_on          = [google_project_service.required]

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.worker[each.key].email
      timeout         = "900s"
      max_retries     = 3

      dynamic "volumes" {
        for_each = local.worker_volume_names[each.key]
        content {
          name = volumes.value

          dynamic "cloud_sql_instance" {
            for_each = volumes.value == "cloudsql" ? [true] : []
            content {
              instances = [google_sql_database_instance.postgres.connection_name]
            }
          }

          dynamic "gcs" {
            for_each = volumes.value == "collection-inputs" ? [true] : []
            content {
              bucket    = google_storage_bucket.collection_inputs.name
              read_only = true
            }
          }
        }
      }

      containers {
        image   = var.image
        args    = [each.key]
        command = []

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        dynamic "env" {
          for_each = local.worker_plain_environment[each.key]
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = local.worker_secret_ids[each.key]
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value
                version = "latest"
              }
            }
          }
        }

        dynamic "volume_mounts" {
          for_each = local.worker_volume_names[each.key]
          content {
            name       = volume_mounts.value
            mount_path = volume_mounts.value == "cloudsql" ? "/cloudsql" : "/collection-inputs"
          }
        }
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}
