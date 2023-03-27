plugins {
    id("java-library")
    id("maven-publish")
}

group = "com.netapp.spark"
version = "1.0.0"

repositories {
    mavenCentral()
}

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("--enable-preview") }

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.3.2")
    implementation("org.apache.spark:spark-sql_2.12:3.3.2")
    implementation("io.undertow:undertow-core:2.3.4.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmit"
            version = "1.0.0"

            from(components["java"])
        }
        /*create<IvyPublication>("ivy") {
            organisation = "com.netapp.spark"
            module = "codesubmit"
            revision = "1.0"

            from(components["java"])
        }*/
    }
    repositories {
        maven {
            url = uri(layout.projectDirectory.dir("repo"))
            //val homeDir = System.getProperty("user.home")
            //url = uri("$homeDir/.ivy2/cache")
        }
    }
}

tasks.test {
    useJUnitPlatform()
}