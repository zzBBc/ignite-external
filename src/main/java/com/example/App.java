package com.example;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import com.mysql.cj.jdbc.MysqlDataSource;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        CacheConfiguration<Integer, Person> personCacheCfg = new CacheConfiguration<>();

        personCacheCfg.setName("PersonCache");
        personCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        personCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        personCacheCfg.setReadThrough(true);
        personCacheCfg.setWriteThrough(true);

        CacheJdbcPojoStoreFactory<Integer, Person> factory = new CacheJdbcPojoStoreFactory<>();
        factory.setDialect(new MySQLDialect());
        factory.setDataSourceFactory( () -> {
            MysqlDataSource mysqlDataSrc = new MysqlDataSource();
            mysqlDataSrc.setURL("jdbc:mysql://localhost:3306/my_db_schema?allowPublicKeyRetrieval=true&useSSL=false");
            mysqlDataSrc.setUser("root");
            mysqlDataSrc.setPassword("123456");
            return mysqlDataSrc;
        });

        JdbcType personType = new JdbcType();
        personType.setCacheName("PersonCache");
        personType.setKeyType(Integer.class);
        personType.setValueType(Person.class);
        // Specify the schema if applicable
        personType.setDatabaseSchema("my_db_schema");
        personType.setDatabaseTable("person");

        personType
                .setKeyFields(new JdbcTypeField(java.sql.Types.INTEGER, "id", Integer.class, "id"));

        personType.setValueFields(
                new JdbcTypeField(java.sql.Types.INTEGER, "id", Integer.class, "id"));
        personType.setValueFields(
                new JdbcTypeField(java.sql.Types.VARCHAR, "name", String.class, "name"));

        factory.setTypes(personType);

        personCacheCfg.setCacheStoreFactory(factory);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(Person.class.getName());
        qryEntity.setKeyFieldName("id");

        Set<String> keyFields = new HashSet<>();
        keyFields.add("id");
        qryEntity.setKeyFields(keyFields);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("name", "java.lang.String");

        qryEntity.setFields(fields);

        personCacheCfg.setQueryEntities(Collections.singletonList(qryEntity));

        igniteCfg.setCacheConfiguration(personCacheCfg);

        Ignite ignite = Ignition.start(igniteCfg);
        // Load data from person table into PersonCache.
        IgniteCache<Integer, Person> personCache = ignite.cache("PersonCache");

        personCache.loadCache(null);

        System.out.println(personCache.get(1).getName());
        personCache.getAndPut(1, new Person(1, "Hoàng"));
        System.out.println(personCache.get(1).getName());

        while(true){
            System.out.println(personCache.get(1).getName());
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
