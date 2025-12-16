apt install jq
# install brew
brew install jq
brew install parquet-cli

echo ". ~/.profile" >> ~/.bash_profile


export NO_PROXY=*
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp dev/my-apache-beam-dataflow/target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar gs://BUCKET/compute/my-apache-beam-dataflow-0.1-SNAPSHOT.jar

Running Python 3.13 installer, you may be prompted for sudo password...
/Library/Developer/CommandLineTools
Xcode Command Line Tools is already installed.
Password:
installer: Package name is Python
installer: Installing at base path /
installer: The install was successful.
Setting up virtual environment
Creating virtualenv...
Installing modules...
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
  Building wheel for crcmod (pyproject.toml) ... done
Virtual env enabled.

Update done!
/Library/Frameworks/Python.framework/Versions/3.13/Resources/Python.app/Contents/MacOS/Python
