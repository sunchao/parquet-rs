#!/bin/bash

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

