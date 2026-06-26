pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven(url = uri(".local-maven"))
        google()
        mavenCentral()
    }
}

rootProject.name = "CrossWatchCompanion"
include(":app")
