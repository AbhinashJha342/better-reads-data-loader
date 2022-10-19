package com.example.cassandra.betterreadsdataloader.repository;

import com.example.cassandra.betterreadsdataloader.schema.Author;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorRepository extends CassandraRepository<Author, String> {


}
