require 'json'
require 'open-uri'
require 'yaml'
require 'sequel'
require 'grackle'

pip = rand(1000000000)
un = 496249821

#twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?screen_name=#{un}").read)

class TweetPull
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

  def userTwits 
    @all_oauth = YAML.load_file("smtp.yml")
    moos = rand(1000000000)
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
    @twits = twits
  end

  def splitTwitToApp
    begin
#      puts 'Pulls the twits'
      userTwits 
#      puts 'Open the database'
      dataConnect
      twit = @twits[0]['user']
      @usersAdd = twit
#      puts "Inserts user #{twit['name']} into Users table"
      dbAppUsers @usersAdd
      @k = 0              
#      puts "Inserts tweets for  #{twit['name']} into Tweets table"
      @twits.each do |f|
	f.delete('user')
        fixed = {'id' => f['id'], 'user_id' => @usersAdd['id']}
	@tweetCol = fixed.merge(f)
        dbAppTweets @tweetCol 

      end
      @i = @i + 1
    rescue
#      puts "Bad Roll"
      @j = @j + 1
      @k = @k + 1
    end
  end

  def dataConnect
    @DB = Sequel.postgres(@all_oauth["Post"]["DB"].to_s, 
			  user: @all_oauth['Post']["User"].to_s, 
			  password: @all_oauth['Post']["Pass"].to_s, 
			  host: @all_oauth['Post']['Host'].to_s, 
			  port:@all_oauth['Post']['Port'], 
			  max_connections: 10)
  end

  def dbAppUsers uncle
    bill = @DB[:twitter__users].filter(:id => uncle['id']).map(:id)
    if bill != []
      @DB[:twitter__users].update(uncle)
#      puts '   ' + uncle['name'] + ' updated'
    else
      @DB[:twitter__users].insert(uncle)
#      puts '   ' + uncle['name'] + ' created'
    end
  end

  def dbAppTweets uncle
    jill = @DB[:twitter__tweets].filter(:id => uncle['id']).map(:id)
    unless jill != []
      @DB[:twitter__tweets].insert(uncle)
#      puts '   ' + uncle['text'] + ' created'
    end
  end

  def userTwit moos
    begin
      puts
      twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
        #puts twits
        #puts 'Tweets from ' + twits[0]['user']['screen_name']
      @users = twits[0]['user']
      
   rescue
	puts "User not found"
    end
  end

  def crackGracklePost message
    begin
      
      client = Grackle::Client.new(
       :auth=>{
        :type=>:oauth,
        consumer_key: @all_oauth['Twit']["Consumer_key"].to_s,
        consumer_secret: @all_oauth['Twit']["Consumer_secret"].to_s,
        token: @all_oauth['Twit']["Access_token"].to_s, 
        token_secret: @all_oauth['Twit']["Access_token_secret"].to_s
        }, 
        :handlers=>{:json=>Grackle::Handlers::StringHandler.new }
      )
      client.statuses.update.json! :status=>message #POST to http://twitter.com/statuses/update.json
    return
      puts "Message Error"
    end
  end 

  def countKill a, b, c, d, e
    t = Time.now - d
    @message =  "  Out of #{a} attempts #{b} catches  with #{c} bad rolls \n  Run over #{t} seconds \n  K hits #{e}"
    puts @message
    crackGracklePost @message
  end

  def twitterPulls x
    start_time = Time.now
    @i = 0
    @j = 0
    @k = 0
    x.times do  
      splitTwitToApp
      if @k >= 15
        countKill x, @i, @j, start_time, @k
        return  
      end
    end
    countKill x, @i, @j, start_time, @k
  end

end

pug = TweetPull.new


pug.twitterPulls 500 
