# Load all required gems
require "rubygems"
require "jdbc/mysql"
require "java"

class DB
  def readProps(props_file)
    properties = {}
    File.open(props_file, 'r') do |f|
      f.read.each_line do |line|
        line.strip!
        if (line[0] != ?# and line[0] != ?=)
          i = line.index('=')
          if (i)
            properties[line[0..i - 1].strip] = line[i + 1..-1].strip
          else
            properties[line] = ''
          end
        end
      end
    end
    properties
  end

  def initialize(dbprops_file)
    @dbprops = readProps(dbprops_file)
    #@mysql_runner="mysql -u#{@dbprops['db.user']} -p#{@dbprops['db.password']} -h#{@dbprops['db.url']} #{@dbprops['db.dbname']}"
    @conn = get_connection
  end

  def get_connection
    if @conn == nil
      # Prep the connection
      Jdbc::MySQL.load_driver
      Java::com.mysql.jdbc.Driver
      #connect_method = java.sql.DriverManager.java_send(:get_connection, [java.lang.String, java.lang.String, java.lang.String])
      #@conn = connect_method.call(@dbprops['db.url'], @dbprops['db.user'], @dbprops['db.password'])
      dmprops = java.util.Properties.new
      dmprops.set_property("user", @dbprops['db.user'])
      dmprops.set_property("password", @dbprops['db.password'])
      @conn = java.sql.DriverManager.get_connection(@dbprops['db.url'], dmprops)
    end
    @conn
  end

  # Define the query
  selectquery = "SELECT * from model order by name"

  def exec_query(sql)
    outarr = Array.new
    conn = get_connection
    begin
      stmt = conn.create_statement
      # Execute the query
      rs = stmt.execute_query(sql)
      while rs.next do
        row = Hash.new
        rsmeta = rs.get_meta_data
        (1..rsmeta.get_column_count).each do |ix|
          row[rsmeta.get_column_name(ix)] = rs.get_object(ix)
        end
        outarr <<= row
      end
    rescue
      $stderr.puts "Hey we failed here.. #{$!}"
    ensure
      stmt.close
    end
    outarr
  end

end

@dbprops_file=if ARGV.length > 0 then
                ARGV[0]
              else
                "/shared/git2/etl/appminer/hive/src/main/resources/db-classint-emr.properties"
              end

db = DB.new(@dbprops_file)
data=db.exec_query("select * from model")
puts data
