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

  def nextInNetwork
    @all_oauth = YAML.load_file("smtp.yml")  
    dataConnect
    #nextNet = @DB[:twitter__tweets].join(:twitter__users, :id => :in_reply_to_user_id).where('users.id is null and in_reply_to_user_id is not null').order(:in_reply_to_user_id).first
    nextNet = @DB['select t.in_reply_to_user_id from twitter.tweets t left join twitter.users u on t.in_reply_to_user_id=u.id left join twitter.no_user n on t.in_reply_to_user_id = n.id where u.id is null and n.id is null and t.in_reply_to_user_id is not null  order by 1 limit 1']
    nextNet = nextNet.map(:in_reply_to_user_id)
    @nextNet =  nextNet[0]
#    puts @nextNet.class.to_s + " #{@nextNet}"
    dataDisconnect    
  end

  def userTwits 
#    moos = rand(1000000000)
    nextInNetwork
    @moos =  @nextNet
    puts "---#{@moos}---#{@moos.class}---"
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{@moos}").read)
    @twits = twits
  end
  def userTwitsT moo 
#    moos = rand(1000000000)
    nextInNetwork
    @moos =  moo
    puts "---#{@moos}---#{@moos.class}---"
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{@moos}").read)
    @twits = twits
  end
  def splitTwitToApp
     begin
      @ll = 0
#      puts 'Pulls the twits'
      userTwits
      @ll += 1
#      puts 'Open the database'

      @ll += 1
      twit = @twits[0]['user']
      @usersAdd = twit
#      puts "Inserts user #{twit['name']} into Users table"
      dbAppUsers @usersAdd
      @ll += 1
      @k = 0              
#      puts "Inserts tweets for  #{twit['name']} into Tweets table"
      @twits.each do |f|
        index = @twits.find_index(f)
#        puts index
        f.delete('user')
	f.delete('coordinates')
	f.delete('geo')
	f.delete('place')
#	puts @twits[index]['coordinates']
	fixed = {'id' => f['id'], 
		'user_id' => @usersAdd['id'],
		'geo' =>  @twits[index]['geo'].to_s,
		'coordinates' => @twits[index]['coordinates'].to_s,
		'place' => @twits[index]['place'].to_s
	}
	@tweetCol = fixed.merge(f)
#        puts @tweetCol
        dbAppTweets @tweetCol 
#        puts "Finished dbAppTweet"
      end
      @ll += 1
      @i += 1
    rescue
      @j += 1
      @k += 1
      nonUserCodes
      bad = {'id' => @moos, 'query_time' => Time.now, 'desc' => @gg}
      dbAppNonUsers bad
    end
  end
  def nonUserCodes
    if @ll == 0
      @gg ="0 Error before userTwist: #{$!}"
    elsif @ll == 1
      @gg = "1 Error before dataConnect: #{$!}"
    elsif @ll == 2
      @gg = "2 Error before dbAppUsers: #{$!}"
    elsif @ll == 3
      @gg = "3 Error before dbAppUsers: #{$!}"
    elsif @ll == 4
      @gg = "4 no error ?: #{$!}"
    else
      @gg = "#{ll} unexpected result: #{$!}"
    end
  end
  def splitTwitToAppT
#     begin
#      puts 'Pulls the twits'
      userTwitsT 2015
#      puts 'Open the database'
      dataConnect
      twit = @twits[0]['user']
      @usersAdd = twit
#      puts "Inserts user #{twit['name']} into Users table"
      dbAppUsers @usersAdd
      @k = 0              
#      puts "Inserts tweets for  #{twit['name']} into Tweets table"

      @twits.each do |f|
        index = @twits.find_index(f)
        puts index
        f.delete('user')
	f.delete('coordinates')
	f.delete('geo')
	f.delete('place')
	puts @twits[index]['coordinates']
	fixed = {'id' => f['id'], 
		'user_id' => @usersAdd['id'],
		'geo' =>  @twits[index]['geo'].to_s,
		'coordinates' => @twits[index]['coordinates'].to_s,
		'place' => @twits[index]['place'].to_s
	}
	@tweetCol = fixed.merge(f)
#	puts @tweetCol
        dbAppTweets @tweetCol 
#        puts "Finished dbAppTweet"
      end
      @i += 1
#    rescue
#      @j += 1
#      @k += 1
#      bad = {'id' => @moos, 'query_time' => Time.now}
#      dbAppNonUsers bad
#    end
  end

  def dataConnect
    @DB = Sequel.postgres(@all_oauth["Post"]["DB"].to_s, 
			  user: @all_oauth['Post']["User"].to_s, 
			  password: @all_oauth['Post']["Pass"].to_s, 
			  host: @all_oauth['Post']['Host'].to_s, 
			  port:@all_oauth['Post']['Port'], 
			  max_connections: 50)
  end
  def dataDisconnect
    @DB.disconnect
  end
  def dbAppUsers uncle
    dataConnect
    bill = @DB[:twitter__users].filter(:id => uncle['id']).map(:id)
#    puts "--#{bill[0]}--"
    if bill[0] != nil
      puts "correct tree"
#      @DB[:twitter__users].where(:id => bill[0]).delete
#      @DB[:twitter__users].insert(uncle)
#      @DB[:twitter__users].update(uncle)
#      puts '   ' + uncle['name'] + ' updated'
    else
      @DB[:twitter__users].insert(uncle)
#      puts '   ' + uncle['name'] + ' created'
    end
    dataDisconnect
  end

  def dbAppTweets uncle
    dataConnect	  
    jill = @DB[:twitter__tweets].filter(:id => uncle['id']).map(:id)
    unless jill != []
      @DB[:twitter__tweets].insert(uncle)
#      puts '   ' + uncle['text'] + ' created'
    end
    dataDisconnect
  end
  def dbAppNonUsers uncle
    dataConnect
    @DB[:twitter__no_user].insert(uncle)
    dataDisconnect
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
      puts "Message Error #{$!}"
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


pug.twitterPulls 150

