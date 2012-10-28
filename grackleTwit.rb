require 'json'
require 'open-uri'
require 'yaml'
require 'grackle'

#puts "select a twitter user to stalk... errr display"
#un = gets.chomp
#seed = Time.now.nsec



				 
#File.join( Rails.root, 'config', 'smtp.yml' ) )



pip = rand(1000000000)

un = 496249821
#twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?screen_name=#{un}").read)

def userTwit moos
  begin
    puts
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
    puts 'Tweets from ' + twits[0]['user']['screen_name']
    puts
    twits.each do |chirp|
      puts '  On ' + chirp['created_at'] + ': ' + chirp['text']
    end
  rescue
	puts "User not found"
  end
end

#userTwit un

def crackGracklePost moos, type, message
  begin
    all_oauth = YAML.load_file("smtp.yml")
    client = Grackle::Client.new(
     :auth=>{
      :type=>:oauth,
      consumer_key: all_oauth["Consumer_key"].to_s,
      consumer_secret: all_oauth["Consumer_secret"].to_s,
      token: all_oauth["Access_token"].to_s, 
      token_secret: all_oauth["Access_token_secret"].to_s
      }, 
      :handlers=>{:json=>Grackle::Handlers::StringHandler.new }
    )
    client.statuses.update.json! :status=>message #POST to http://twitter.com/statuses/update.json
  return
    puts "Message Error"
  end
end 


def crackGrackleGet moos, type
  begin
    all_oauth = YAML.load_file("smtp.yml")
    client = Grackle::Client.new(
     :auth=>{
      :type=>:oauth,
      consumer_key: all_oauth["Consumer_key"].to_s,
      consumer_secret: all_oauth["Consumer_secret"].to_s,
      token: all_oauth["Access_token"].to_s, 
      token_secret: all_oauth["Access_token_secret"].to_s
      }, 
      :handlers=>{:json=>Grackle::Handlers::StringHandler.new }
    )
    littleMoo = client.users.show.json? type.to_sym=> moos #http://twitter.com/users/show.json?#{type}=moos
    twits = JSON.parse("#{littleMoo}")
    puts 'Tweets from ' + twits['screen_name']
    puts
    puts '  On ' + twits['status']['created_at'] + ': ' + twits['status']['text']
   rescue
    puts "User not found"
  end
end




puts "no auth"
userTwit un 
puts
puts 'grackle post'
#crackGracklePost un, 'id', '@emilyellison986 the robots are comming for you!'
puts "grackle get"
userTwit un

#crackGrackleName 'PassActivism'
