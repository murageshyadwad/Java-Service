package com.example.demo;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JdbcReadPipeline {
  public interface JdbcReadOptions extends PipelineOptions {
    @Description("JDBC driver class name")
    @Default.String("org.postgresql.Driver")
    String getDriverClassName();
    void setDriverClassName(String value);

    @Description("JDBC database URL")
    @Default.String("jdbc:postgresql://localhost/mydatabase")
    String getDatabaseUrl();
    void setDatabaseUrl(String value);

    @Description("JDBC database username")
    @Default.String("myusername")
    String getDatabaseUsername();
    void setDatabaseUsername(String value);

    @Description("JDBC database password")
    @Default.String("mypassword")
    String getDatabasePassword();
    void setDatabasePassword(String value);
  }

  public static void main(String[] args) {
    JdbcReadOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcReadOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> data = pipeline.apply(JdbcIO.<String>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(options.getDriverClassName(), options.getDatabaseUrl())
            .withUsername(options.getDatabaseUsername())
            .withPassword(options.getDatabasePassword()))
        .withQuery("SELECT * FROM mytable")
        .withCoder(StringUtf8Coder.of())
        .withRowMapper(new JdbcIO.RowMapper<String>() {
          public String mapRow(ResultSet resultSet) throws Exception {
            return resultSet.getString("mycolumn");
          }
        })
        .withStatementPreparator
        (new JdbcIO.PreparedStatementSetter<String>() {
            
              
        public void setParameters(String element, PreparedStatement statement) throws SQLException {
            statement.setString(1, element);
        }
            }));
        

    data.apply(ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        System.out.println(context.element());
      }
    }));

    pipeline.run();
  }
}

