plugins {
    id("java-library")
    id("application")
    id("maven-publish")
    id("org.graalvm.buildtools.native") version "0.9.22"
    id("com.google.cloud.tools.jib") version "3.3.2"
}

group = "com.netapp.spark"
version = "1.0.0"

var theJvmArgs = listOf(
    "--enable-preview",
)

repositories {
    mavenCentral()
}

jib {
    from {
        image = "openjdk:21-jdk"
        //image = "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:baseimage-1.0.0"
        //version = "baseimage-1.0.0"
        platforms {
            platform {
                architecture = "amd64"
                os = "linux"
            }
        }
        if (project.hasProperty("REGISTRY_USER")) {
            auth {
                username = project.findProperty("REGISTRY_USER")?.toString()
                password = project.findProperty("REGISTRY_PASSWORD")?.toString()
            }
        }
    }
    to {
        image = project.findProperty("APPLICATION_REPOSITORY")?.toString() ?: "public.ecr.aws/l8m2k1n1/netapp/spark/notebookinit:1.0.0"
        //version = "1.0.0"
        //tags = [project.findProperty("APPLICATION_TAG")?.toString() ?: "1.0"]
        if (project.hasProperty("REGISTRY_USER")) {
            val reg_user = project.findProperty("REGISTRY_USER")?.toString()
            val reg_pass = project.findProperty("REGISTRY_PASSWORD")?.toString()
            auth {
                username = reg_user
                password = reg_pass
            }
        }
    }
    container {
        mainClass = "com.netapp.spark.NotebookInitContainer"
        jvmFlags = theJvmArgs
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.4.0")
    implementation("org.apache.spark:spark-connect_2.12:3.4.0")
    implementation("org.apache.spark:spark-sql_2.12:3.4.0")
    implementation("org.apache.spark:spark-repl_2.12:3.4.0")
    implementation("org.apache.spark:spark-hive_2.12:3.4.0")
    implementation("org.apache.spark:spark-hive-thriftserver_2.12:3.4.0")
    implementation("io.undertow:undertow-core:2.3.5.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    implementation("org.apache.commons:commons-compress:1.23.0")
    implementation("io.kubernetes:client-java:18.0.0")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("io.fabric8:kubernetes-client:6.7.1")
    implementation("org.eclipse.jgit:org.eclipse.jgit:6.6.0.202305301015-r")

    implementation(files("src/main/jib/opt/spark/jars/toree.jar"))

    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmission"
            version = "1.0.0"

            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri(layout.projectDirectory.dir("repo"))
        }
    }
}

application {
    mainClass.set("com.netapp.spark.SparkCodeSubmissionServer")
    applicationDefaultJvmArgs = theJvmArgs
}

tasks {
    val fatJar = register<Jar>("fatJar") {
        setProperty("zip64",true)
        dependsOn.addAll(listOf("compileJava", "processResources")) // We need this for Gradle optimization to work
        archiveClassifier.set("sparkcodesubmissionplugin") // Naming the jar
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        val sourcesMain = sourceSets.main.get()
        val contents = configurations.runtimeClasspath.get()
            .map { if (it.isDirectory) it else zipTree(it) } +
                sourcesMain.output
        from(contents)
    }
    build {
        dependsOn(fatJar)
    }
}

tasks.test {
    jvmArgs = theJvmArgs
    useJUnitPlatform()
}

graalvmNative {
    binaries {
        named("main") {
            mainClass.set("com.netapp.spark.NotebookInitContainer")
            jvmArgs.addAll(theJvmArgs)
            useFatJar.set(true)
        }
    }
}