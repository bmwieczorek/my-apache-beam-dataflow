apt install jq
# install brew
brew install jq
brew install parquet-cli

echo ". ~/.profile" >> ~/.bash_profile

# generate .python-version with composer-2.4.3-airflow-2.5.3-python-3.8.14-jupyter to start terminal with virtualenv
pyenv local composer-2.4.3-airflow-2.5.3-python-3.8.14-jupyter

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

VS Code test @hidden

Disabling Intellij Inspections:
1. Probable Bugs -> Nullability problems -> @NotNull/@Nullable problems -> uncheck Report non-annotated parameters or methods overriding @NotNull

2. Declaration redundancy -> Unused declaration -> Configure Code Patterns -> + check with subclasses, class: org.apache.beam.options.PipelineOptions, method: set*

# Get job details:

curl -X GET \
 -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 -H "Content-Type: application/json; charset=utf-8" \
"https://dataflow.googleapis.com/v1b3/projects/my-project-id/locations/my-region/jobs/my-job-id?view=JOB_VIEW_ALL"

# VS Code keybindings
me@MacBook:~$ cp ~/Library/Application\ Support/Code/User/keybindings.json ~/dev/my-apache-beam-dataflow/keybindings.json 

# VS Code Java Extension Settings
File: .settings/org.eclipse.jdt.core.prefs

# VS Code Java Extension Settings not to complain about @SuppressWarnings("unused")
org.eclipse.jdt.core.compiler.problem.unusedWarningToken=ignore