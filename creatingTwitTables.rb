require 'json'
require 'open-uri'
require 'yaml'
require 'sequel'

pip = rand(1000000000)
un = 496249821

#twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?screen_name=#{un}").read)

class TwitPull
  @aUto = []  	
  def classToType a, b
    if a.class == Fixnum
      if b.key(a) == 'id'
        return 'int PRIMARY KEY'
      else
        return 'int'	    
      end
    elsif a.class == Float
      return 'float8'
    elsif a.class == TrueClass 
      return 'bool'
    elsif a.class == FalseClass 
      return 'bool'    
    else
      return 'text'
    end
  end

  def userTwits moos
    #moos = rand(1000000000)
    #puts moos
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
    @twits = twits
    #puts @twits[0].class.to_s
  end

  def splitTwitToCol
    begin
      userTwits 496249821
#      puts 'Tweets from ' + @twits[0]['user']['screen_name']
      twit = @twits[0]['user']
      chirp = @twits[0]
      @usersCol = 'Create table twitter.users ( id' + ' ' + classToType(twit['id'],twit)
#      puts @usersCol
      twit.keys.each do |j|
        unless j == 'id'
          @usersCol = @usersCol + ', ' + j + ' ' + classToType(twit[j.to_s],twit).to_s
	end
      end
      @usersCol = @usersCol + ');'
      puts @usersCol
      @tweetCol = 'Create table twitter.tweets ( id' + ' ' + classToType(chirp['id'],chirp) + ', user_id int'
      chirp.keys.each do |f|
        unless f == 'user' || f == 'id'
	  @tweetCol = @tweetCol + ', ' + f + ' ' + classToType(chirp[f.to_s],chirp).to_s 
        end
      end
      @tweetCol = @tweetCol + ');'
      puts
      puts @tweetCol
    rescue
      puts "User not found"
    end
  end

  def userTwit moos
    begin
      puts
      twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
      puts 'Tweets from ' + twits[0]['user']['screen_name']
      twit = twits[0]['user']
      twit.keys.each do |j|
        puts '  ' + j + ': ' + twit[j.to_s].to_s + ' ' + twit[j.to_s].class.to_s + ' ' + classToType(twit[j.to_s],twit)
      end
      puts
      twits.each do |chirp|
        chirp.keys.each do |f|
          unless f == 'user'
            puts '    ' + f + ': ' + chirp[f.to_s].to_s + ' ' + twit[f.to_s].class.to_s  + ' ' + classToType(twit[f.to_s],twit)
	  end
        
        end
        puts 
      end
   rescue
	puts "User not found"
    end
  end
  def dataConnect
    @DB = Sequel.postgres('postgres', user: 'rubybot', password: 'rubyBot', host: 'localhost', port: 5432, max_connections: 10)
  end
  def dbRun uncle
    @DB.run(uncle)
  end
  def dbCreate
    dbRun "#{@usersCol}"
    dbRun "#{@tweetCol}"
  end  
end
#userTwit un

#DB = Sequel.postgres('postgres', user: 'rubybot', password: 'rubyBot', host: 'localhost', port: 5432, max_connections: 10)
current_time = Time.now
#.strftime('%D')

puts
puts current_time


pug = TwitPull.new

#DB.run("create table twitter.t (a text, b text)")
#DB.run("insert into twitter.t values ('a', 'b')")
boo = {'id' => 23424,'b' => 7.234234234}
#puts pug.classToType boo['b'], boo
#puts pug.classToType boo['id'], boo



puts 'Pulls the twits'
pug.splitTwitToCol
puts 'Open the database'
pug.dataConnect
puts 'runs the data'
pug.dbCreate
puts 'Something happened'
