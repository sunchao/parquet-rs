#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

GITHUB_USER="doc-deploy-bot"
GITHUB_EMAIL=""

DOC_VERSION=${TRAVIS_TAG:-$TRAVIS_BRANCH}
if [ -z "$DOC_VERSION" ]; then
  echo "Failed to find documentation version, make sure TRAVIS_TAG or TRAVIS_BRANCH is set"
  exit 1
fi

echo "Deploying documentation version $DOC_VERSION"

# Check that GITHUB_TOKEN is set
if [ -z "$GITHUB_TOKEN" ]; then
  echo "GITHUB_TOKEN is not set.
        You’ll need to generate a personal access token with the 'public_repo'.
        Since the token should be private, you’ll want to pass it to Travis securely
        in your repository settings or via encrypted variables in .travis.yml."
  exit 1
fi

if [ -d "target/doc" ]; then
  echo "<meta http-equiv=refresh content=0;url=parquet/index.html>" > ./target/doc/index.html

  echo "Setting up gh-pages branch" &&
  git clone --branch gh-pages "https://$GITHUB_TOKEN@github.com/${TRAVIS_REPO_SLUG}.git" deploy_docs > /dev/null 2>&1 &&
  cd deploy_docs &&
  rm -rf ./$DOC_VERSION &&
  mkdir ./$DOC_VERSION &&
  mv ../target/doc/* ./$DOC_VERSION/ &&
  git config user.name "$GITHUB_USER" &&
  git config user.email "$GITHUB_EMAIL" &&
  git add -A . &&
  git commit -m "Deploy doc pages $DOC_VERSION at ${TRAVIS_COMMIT}" &&
  echo "Pushing documentation update" &&
  git push --quiet origin gh-pages > /dev/null 2>&1 &&
  echo "Published documentation $DOC_VERSION" || echo "Failed to publish documentation $DOC_VERSION"
else
  echo "Failed to find target/doc directory, have you built the docs?"
  exit 1
fi

