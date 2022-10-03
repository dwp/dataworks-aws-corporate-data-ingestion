#!/usr/bin/env bash

get_release_information(){
    VERSION=$(curl --silent "https://api.github.com/repos/dwp/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    RESPONSE=$(curl --silent "https://api.github.com/repos/dwp/${REPO}/releases/latest")
}

update_tfvars() {
    tfvars_line=$(echo "$(echo ${REPO} | sed 's/-/_/g')_$extension = { base_path = \"../${REPO}-release\", version = \"${VERSION}\" }")
    if ! cat terraform.tfvars | grep -q "$tfvars_line"; then
        echo "$tfvars_line" >> terraform.tfvars
    fi
}

get_release() {
    get_release_information
    for k in $(jq '.assets | keys | .[]' <<< "$RESPONSE"); do
        value=$(jq -r ".assets[$k]" <<< "$RESPONSE");
        url=$(jq -r ".browser_download_url" <<< "$value");
        RELEASE="${REPO}-${VERSION}"
        if  echo "$url" | grep -q "$RELEASE"; then
            export ASSET=$url
            filename=$(basename -- "$ASSET")
            extension="${filename##*.}"
            fetch_asset
        fi
    done
}

fetch_asset(){
    EXISTING_VERSION=$(ls ../${REPO}-release/ | grep "${VERSION}")

    if [[ -f ../${REPO}-release/$EXISTING_VERSION ]]; then
        echo "${REPO}-release/$EXISTING_VERSION already exists: Skipping download"
    else
        mkdir "../${REPO}-release"
        (cd "../${REPO}-release/" && curl -L -O "${ASSET}")
    fi

    update_tfvars
}

get_release

