# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# RUN BUILD SERVER

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run //store:store_image

while true; do docker-compose logs -f store; done

# K8 commands

kubectl apply -f ./resources/

kubectl rollout restart deployment store

kubectl create configmap nginx-config --from-file=nginx.conf

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run //store:image_push