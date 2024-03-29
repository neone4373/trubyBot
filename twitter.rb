require 'json'
require 'open-uri'
require 'yaml'
require 'sequel'
require 'grackle'

un = 496249821

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
    nextNet = @DB['
      select 
        t.in_reply_to_user_id 
      from twitter.tweets t 
        left join twitter.users u 
          on t.in_reply_to_user_id=u.id 
        left join twitter.no_user n 
          on t.in_reply_to_user_id = n.id 
      where u.id is null and n.id is null 
        and t.in_reply_to_user_id is not null  
      group by 1
      order by 1 
      --limit 1
    ']
    nextNet = nextNet.map(:in_reply_to_user_id)
    @nextNet =  nextNet.sample
    puts @nextNet.class.to_s + " ID #{@nextNet} "
    dataDisconnect    
  end
  def crackGrackleGet moos
    crackGracleClient
    littleMoo = @client.statuses.user_timeline.json? :id => moos 
    #http://twitter.com/statuses/user_timeline.json?id=moos
    @twits = JSON.parse("#{littleMoo}")
    #puts 'Tweets from ' + @twits[0]['user']['screen_name']
    @callsLeft = @client.response.headers["X-Ratelimit-Remaining"].to_s
    #puts @callsLeft
    #puts
    #puts twits.to_s
    #@twits.each do |chirp|
      #puts '  On ' + chirp['created_at'] + ': ' + chirp['text']
    #end
  end
  def crackGracklePost message
    begin
    crackGracleClient      
    @client.statuses.update.json! :status=>message #POST to http://twitter.com/statuses/update.json
    return
      puts "Message Error #{$!}"
    end
  end 
  def userTwits 
    nextInNetwork
    crackGrackleGet @nextNet
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
     @h += 1
     begin
      @ll = 0
      userTwits
      @ll += 1
      @ll += 1
      twit = @twits[0]['user']
      @usersAdd = twit
      dbAppUsers @usersAdd
      @ll += 1
      @k = 0              
      @twits.each do |f|
        index = @twits.find_index(f)
        f.delete('user')
	f.delete('coordinates')
	f.delete('geo')
	f.delete('place')
	fixed = {'id' => f['id'], 
          'user_id' => @usersAdd['id'],
          'geo' =>  @twits[index]['geo'].to_s,
          'coordinates' => @twits[index]['coordinates'].to_s,
	  'place' => @twits[index]['place'].to_s
	}
	@tweetCol = fixed.merge(f)
        dbAppTweets @tweetCol 
      end
      @ll += 1
      @i += 1
    rescue
      @j += 1
      @k += 1
      nonUserCodes
      bad = {'id' => @nextNet, 'query_time' => Time.now, 'desc' => @gg}
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
#      puts "correct tree"
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

  def crackGracleClient
     @client = Grackle::Client.new(
       :auth=>{
       :type=>:oauth,
         consumer_key: @all_oauth['Twit']["Consumer_key"].to_s,
         consumer_secret: @all_oauth['Twit']["Consumer_secret"].to_s,
         token: @all_oauth['Twit']["Access_token"].to_s, 
         token_secret: @all_oauth['Twit']["Access_token_secret"].to_s
       }, 
       :handlers=>{:json=>Grackle::Handlers::StringHandler.new }
      )
  end


  def countKill a, b, c, d, e, f
    t = Time.now - d
    @message =  "  Out of #{a} attempts #{b} catches  with #{c} bad rolls 
    \n  Run over #{t} seconds 
    \n  K hits #{e} 
    \n  #{f} calls left"
    puts @message
      crackGracklePost @message
  end

  def twitterPulls x
    start_time = Time.now
    @h = 0
    @i = 0
    @j = 0
    @k = 0
    x.times do  
      splitTwitToApp
      if @k >= 15 || @callsLeft == 0
        countKill x, @i, @j, start_time, @k, @callsLeft
        return  
      end
    end
    countKill x, @i, @j, start_time, @k, @callsLeft
  end
end

pug = TweetPull.new


pug.twitterPulls 350

