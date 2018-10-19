////////////////////////////////////////////////////////////////////////////////////////////////////////
// CONFIGURATION VARIABLES
////////////////////////////////////////////////////////////////////////////////////////////////////////

// Branch names for production and staging
def BRANCH_PROD = "release"
def BRANCH_STAG = "master"

// AWS Account ID
def AWS_ACCOUNT = "735245989296"

// AWS Region
def AWS_REGION = "eu-central-1"

// ECS Image repository (common for all)
def IMAGE_REPOSITORY = "tracker"

// Tags on the docker image repository
def IMAGE_TAG_PROD = "release"
def IMAGE_TAG_STAG = "latest"
def IMAGE_TAG_TEST = "latest"

// ECS cluster name
def ECS_CLUST_PROD = "ecs-tracker-production"
def ECS_CLUST_STAG = "ecs-tracker-staging"
def ECS_CLUST_TEST = "ecs-tracker-staging"

// ECS service name
def ECS_SERVI_PROD = "tracker-production"
def ECS_SERVI_STAG = "tracker-staging"
def ECS_SERVI_TEST = "tracker-staging"

// Colors for slack messages
def SLACK_COLOR_STARTED = '#D4DADF'
def SLACK_COLOR_SUCCESS = '#BDFFC3'
def SLACK_COLOR_FAILURE = '#FF9FA1'

// This limits the autobuilding to master on staging only.
// Result of the expression must be 'master' for master branch and a non existant branchname else
def BRANCH_FILTER = "${(env.BRANCH_NAME == BRANCH_STAG) ? BRANCH_STAG : 'nSjHmdnAdgkje2153'}"

// Tracker Repository URL
def TRACKER_URL = "github.com/ichliebedich/tracker"

// Docker File to build
def DOCKER_FILE = "Dockerfile.homodea"

////////////////////////////////////////////////////////////////////////////////////////////////////////
// PIPELINE
////////////////////////////////////////////////////////////////////////////////////////////////////////

properties([
  [$class: 'GithubProjectProperty', displayName: '', projectUrlStr: 'https://github.com/ichliebedich/tracker/']
])

pipeline {
  agent any
  
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '8'))
  }
  
  triggers {
    githubBranches events: [hashChanged(), restriction(exclude: 'false', matchAsPattern: 'false', matchCriteriaStr: "${BRANCH_FILTER}")], preStatus: true, spec: '', triggerMode: 'HEAVY_HOOKS'
  }
  
  environment {
    IMAGE_REP = "${IMAGE_REPOSITORY}"
    IMAGE_TAG = "${(env.BRANCH_NAME == BRANCH_PROD) ? IMAGE_TAG_PROD : (env.BRANCH_NAME == BRANCH_STAG) ? IMAGE_TAG_STAG : IMAGE_TAG_TEST}"
    ECS_CLUST = "${(env.BRANCH_NAME == BRANCH_PROD) ? ECS_CLUST_PROD : (env.BRANCH_NAME == BRANCH_STAG) ? ECS_CLUST_STAG : ECS_CLUST_TEST}"
    ECS_SERVI = "${(env.BRANCH_NAME == BRANCH_PROD) ? ECS_SERVI_PROD : (env.BRANCH_NAME == BRANCH_STAG) ? ECS_SERVI_STAG : ECS_SERVI_TEST}"
    ECR_DESTI = "${AWS_ACCOUNT + '.dkr.ecr.' + AWS_REGION + '.amazonaws.com/' + env.IMAGE_REP + ':' + env.IMAGE_TAG}"
    AWS_REGIO = "${AWS_REGION}"
    TRACK_URL = "${TRACKER_URL}"
    DOCK_FILE = "${DOCKER_FILE}"
  }

  stages {
    stage ('prepare') {
      steps {
        slackSend(color: "${SLACK_COLOR_STARTED}", message: "STARTED: `${env.JOB_NAME}` #${env.BUILD_NUMBER}:\n${env.BUILD_URL}")
      }
    }
    stage('build') {
      steps {
        sh """
          go get ${TRACK_URL}
          go install ${TRACK_URL}
          go build -o ./tracker
          docker build -f ${DOCK_FILE} -t ${IMAGE_REP}:${IMAGE_TAG} .
          docker tag ${IMAGE_REP}:${IMAGE_TAG} ${ECR_DESTI}
        """
      }
    }
    stage('deploy') {
      steps {
        sh """
          eval \$(aws ecr get-login --no-include-email --region ${AWS_REGIO})
          docker push ${ECR_DESTI}
          aws ecs update-service --force-new-deployment --cluster ${ECS_CLUST} --service '${ECS_SERVI}'
          aws ecs update-service --force-new-deployment --cluster ${ECS_CLUST} --service '${ECS_SERVI}-2'
        """
      }
    }
  }
    
  post {
    success {
      slackSend(color: "${SLACK_COLOR_SUCCESS}", message: "SUCCESS: `${env.JOB_NAME}` #${env.BUILD_NUMBER}:\n${env.BUILD_URL}")
    }
    failure {
      slackSend(color: "${SLACK_COLOR_FAILURE}", message: "FAILURE: `${env.JOB_NAME}` #${env.BUILD_NUMBER}:\n${env.BUILD_URL}")
    }
  }
}
