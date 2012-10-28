require 'rubygems'
require 'sequel'

DB = Sequel.postgres('Work', user: 'rubybot', password: 'rubyBot', host: 'localhost', port: 5432, max_connections: 10)
current_time = Time.now
#.strftime('%D')

puts
puts current_time

DB.run("drop table if exists riviera.rubybot_first")
DB.run("create table riviera.rubybot_first (PRIMARY KEY, created_on timestamp with time zone, comment text)")
DB.run("insert into riviera.rubybot_first VALUES (1,'#{current_time}', 'Hello from rubyBot!')")
tbl = DB['select * from riviera.rubybot_first limit 1']
puts
puts "inside table " + tbl.to_s
