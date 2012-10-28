require 'json'
require 'open-uri'
require 'yaml'
require 'sequel'

pip = rand(1000000000)

un = 496249821
#twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?screen_name=#{un}").read)

def userTwit moos
  begin
    puts
    twits = JSON.parse(open("http://api.twitter.com/1/statuses/user_timeline.json?id=#{moos}").read)
    puts 'Tweets from ' + twits[0]['user']['screen_name']
    twit = twits[0]['user']
    twit.keys.each do |j|
      puts '  ' + j + ': ' + twit[j.to_s].to_s
    end
    puts
    twits.each do |chirp|
      chirp.keys.each do |f|
        unless f == 'user'
          puts '    ' + f + ': ' + chirp[f.to_s].to_s 
	end
        
      end
      puts 
    end
 rescue
	puts "User not found"
  end
end

userTwit pip

