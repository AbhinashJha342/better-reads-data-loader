package com.example.cassandra.betterreadsdataloader;

import com.example.cassandra.betterreadsdataloader.repository.BookRepository;
import com.example.cassandra.betterreadsdataloader.schema.Author;
import com.example.cassandra.betterreadsdataloader.configurations.DataStaxAstraProperties;
import com.example.cassandra.betterreadsdataloader.repository.AuthorRepository;
import com.example.cassandra.betterreadsdataloader.schema.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpsLocation;

    @Value("${datadump.location.works}")
    private String worksDumpsLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
    }


    @PostConstruct
    public void start(){
        initAuthors();
        initWorks();
        /*Author author = new Author();
        author.setId("id");
        author.setName("author name");
        author.setPersonalName("author personal name");
        authorRepository.save(author);*/
    }


    private void initAuthors(){
        Path path = Paths.get(authorDumpsLocation);
        try(Stream<String> lines = Files.lines(path)){
            lines.forEach(line -> {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                // Construct author object
                // Persist using Repository
            });
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    private void initWorks(){
        Path path = Paths.get(worksDumpsLocation);
        try(Stream<String> lines = Files.lines(path)){
            lines.forEach(line -> {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                JSONObject jsonObject = null;
                try {
                    jsonObject = new JSONObject(jsonString);
                    Book book = new Book();

                } catch (JSONException e) {
                    e.printStackTrace();
                }
                // Construct author object
                // Persist using Repository
            });
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties){
        Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
