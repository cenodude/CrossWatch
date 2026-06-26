buildscript {
    repositories {
        maven(url = uri(".local-maven"))
        google()
        mavenCentral()
    }
    dependencies {
        classpath("com.android.tools.build:gradle:9.2.1")
    }
}
