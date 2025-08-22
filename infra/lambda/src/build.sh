# build.sh
cd src
pip install -r ../requirements.txt -t .
zip -r ../lambda_function.zip .
