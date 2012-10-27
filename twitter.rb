require 'json'
require 'open-uri'

#puts "select a twitter user to stalk... errr display"
#un = gets.chomp
#seed = Time.now.nsec

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
userTwit pip 
