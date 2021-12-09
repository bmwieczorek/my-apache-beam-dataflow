terraform {
//  backend "gcs" {
//    bucket = "bartek-terraform"
//    prefix = "bartek-mypubsubtogcsjob/tfstate"
//  }
  required_providers {
    google = {
      version = "~> 3.58.0"
    }
  }
  required_version = "~> 0.12.29"
}
