# AI Job Market Analysis 

## Requirements: 
- IntelliJ IDEA
- Java 17
- Maven
- Scala 2.12
- Apache Spark 3.5.1

## Setup Instructions:
1. Clone the repository.
2. Open the project in IntelliJ. 
3. When opened, you will most likely be prompted to download/reload Maven. Please do so.
4. Ensure Project SDK is Java 17.
   - If you dont have Java 17 installed, follow the steps below:
     1. Download here: https://adoptium.net/temurin/releases/
     2. Download the `.msi` installer and run it.
     3. Verify installation via the command
        ```bash
        java -version
        ```
     5. Configure IntelliJ
    
        
        File -> Project Structure -> Project
        Set Project SDK: Java 17
        Set Language Level: 17

        File -> Project Structure -> Modules
        Set Module SDK: Java 17
        
     7. If everything goes well, the project should now run!
