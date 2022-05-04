locals {
  labels = {
    "owner" : var.owner
  }
  key = "${var.owner}-key"
  key_ring = "${local.key}-ring"
  key_id = "projects/${var.project}/locations/us/keyRings/${local.key_ring}/cryptoKeys/${local.key}"

  sa_roles = {
    "role1" = {
      role = "roles/dataflow.worker"
    },
    "role2" = {
      role = "roles/dataflow.developer"
    },
    "role3" = {
      role = "roles/cloudfunctions.developer"
    },
    "role4" = {
      role = "roles/datafusion.runner"
    },
    "role5" = {
      role = "roles/secretmanager.secretAccessor"
    },
    "role6" = {
      role = "roles/bigquery.jobUser"
    }
  }

  secrets_list = {
    secret1 = {
      secretname  = "username"
      ciphertext  = "..." // echo -n myusername | gcloud kms encrypt --project ${GCP_PROJECT} --location us --keyring ${GCP_OWNER}-key-ring --key ${GCP_OWNER}-key --plaintext-file - --ciphertext-file - | base64
    },
    secret2 = {
      secretname  = "password"
      ciphertext  = "..." // echo -n mypassword | gcloud kms encrypt --project ${GCP_PROJECT} --location us --keyring ${GCP_OWNER}-key-ring --key ${GCP_OWNER}-key --plaintext-file - --ciphertext-file - | base64
    }
  }
}

resource "google_kms_key_ring" "key_ring" {
//  count    = var.provision_keyring == true ? 1 : 0
  name     = local.key_ring
  project  = var.project
  location = "us"
}

resource "google_kms_crypto_key" "key" {
//  count           = var.provision_key == true ? 1 : 0
  name            = local.key
  key_ring        = google_kms_key_ring.key_ring.id
  rotation_period = "7776000s"
  labels          = local.labels
  depends_on      = [ google_kms_key_ring.key_ring]
}

data "google_project" "project" {
  project_id = var.project
}

resource "google_service_account" "main_sa" {
  account_id   =  "${var.owner}-main-sa"
  display_name = "${var.owner} main service account"
  project      = var.project
}

resource "google_project_iam_member" "main_sa_roles" {
  for_each    = local.sa_roles
  project     = var.project
  role        = each.value.role
  member      = "serviceAccount:${google_service_account.main_sa.email}"
}

resource "google_kms_crypto_key_iam_member" "key_encrypter_for_main_sa" {
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  crypto_key_id = local.key_id
  member        = "serviceAccount:${google_service_account.main_sa.email}"
  depends_on = [google_kms_crypto_key.key]
}

resource "google_kms_crypto_key_iam_member" "key_encrypter_for_bq_sa" {
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  crypto_key_id = local.key_id
  member        = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}

resource "google_bigquery_dataset" "dataset_with_key" {
  count                       = 0
  dataset_id                  = "${var.owner}_dataset_with_key"
  location                    = "US"
  project                     = var.project
  labels                      = local.labels
  default_encryption_configuration {
    kms_key_name = local.key_id
  }

  access {
    role          = "OWNER"
    user_by_email = google_service_account.main_sa.email
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  depends_on = [google_kms_crypto_key_iam_member.key_decrypter_for_gs_sa]
}

resource "google_kms_crypto_key_iam_member" "key_decrypter_for_gs_sa" {
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  crypto_key_id = local.key_id
  member        = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

resource "google_storage_bucket" "bucket_with_key" {
  name          = "${var.project}-${var.owner}-bucket-withkey"
  project       = var.project
  location      = "US"
  encryption {
    default_kms_key_name = local.key_id
  }

  depends_on = [
    google_kms_crypto_key_iam_member.key_decrypter_for_gs_sa,
  ]
}

resource "google_storage_bucket_iam_member" "bucket_admin_for_main_sa" {
  bucket   = google_storage_bucket.bucket_with_key.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.main_sa.email}"
}

resource "google_kms_crypto_key_iam_member" "key_encrypter_for_pubsub_sa" {
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  crypto_key_id = local.key_id
  member        = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}


resource "google_pubsub_topic" "topic_with_key" {
  project      = var.project
  name         = "${var.owner}-topic-with-key"
  labels       = local.labels
  kms_key_name = local.key_id
  depends_on   = [google_kms_crypto_key_iam_member.key_encrypter_for_pubsub_sa]
}


resource "google_pubsub_subscription" "subscription_with_key" {
  name    = "${var.owner}-subscription-with-key"
  topic   = google_pubsub_topic.topic_with_key.name
  project = var.project
  labels  = local.labels
}


resource "google_pubsub_topic_iam_member" "pubsub_admin_for_main_sa" {
  project = var.project
  topic   = google_pubsub_topic.topic_with_key.name
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.main_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "subscription_sa_binding" {
  project      = var.project
  subscription = google_pubsub_subscription.subscription_with_key.name
  role         = "roles/pubsub.admin"
  member       = "serviceAccount:${google_service_account.main_sa.email}"
}

#Create secrets in secrets Manager with provided secrets list.
data "google_kms_secret" "secret_cipher" { // stores cipher
  for_each = local.secrets_list
  crypto_key  = local.key_id
  ciphertext  = each.value.ciphertext
}

resource "google_secret_manager_secret" "secret_manager" {  // stores name
  for_each    = local.secrets_list
  secret_id   = each.value.secretname
  labels      = local.labels
  project     = var.project

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_secret_manager_secret_version" "secret_version" { // saves (name,cipher)
  for_each    = local.secrets_list
  secret      = google_secret_manager_secret.secret_manager[each.key].id
  secret_data = data.google_kms_secret.secret_cipher[each.key].plaintext
}

# gcloud secrets versions access latest --project ${GCP_PROJECT} --secret username
# gcloud secrets versions access latest --project ${GCP_PROJECT} --secret password
