require 'fql'
require 'mongo'
require 'thread'

ACCESS_TOKEN = "" # String
GROUP_IDENTIFIER = ; #Number

$options = {access_token: ACCESS_TOKEN}

class Worker

	@@post_query = "SELECT post_id, tagged_ids, actor_id, updated_time, message, comments FROM stream WHERE source_id=#{$GROUP_IDENTIFIER} LIMIT "
	@@mongo = Mongo::MongoClient.new("localhost", 27017)
	@@client = @@mongo.db("mvci")
	@@post = @@client.collection("post")

	def self.makePostQuery(from, to)
		return @@post_query + "#{from}, #{to}"
	end

	def initialize
		@mutex = Mutex.new
		@is_work = false
		@from = 0
		@to  = 0
		@new_work = false

		thread = Thread.new do
			while(true) 
				@mutex.synchronize do
					if @new_work
						_work
						@new_work = false
						@is_work = false
					end
				end

				sleep 0.01
			end
		end

		thread.run
	end

	def work(from, to)
		@mutex.synchronize do
			@from = from
			@to = to
			@is_work = true
			@new_work = true
			p "Reauested #{from} ~ #{to}"
		end
	end

	def is_work
		return @is_work
	end

	def is_work?
		return @is_work
	end

	def _work
		from = @from
		to = @to
		p "processing #{@from} ~ #{@to}"
		result = Fql.execute(Worker.makePostQuery(from, to) ,$options)

		result.each do |item|
			existed = @@post.find_one({"post_id" => item["post_id"]});
			if (existed == nil) 
				@@post.insert(item)
			else
				if item["old"] == nil || item.instance_of?(Array)
					item["old"] = Array.new
				end

				item["old"] << existed

				@@post.update({"post_id" => item["post_id"]}, item)
			end
		end

		p "#{@from} ~ #{@to} End"
	end
end


class WorkerSupervisor

	def initialize(worker_size, limit_size)
		@worker_size = worker_size
		@workers = Array.new
		@now = 1
		@limit_size = 10

		@worker_size.times do
			@workers << Worker.new
		end
	end

	def run
		while(true)
			@workers.each do |item|
				unless item.is_work?
					old = @now
					@now += @limit_size

					item.work(old, @now)
				end
			end

			sleep 0.01
		end
	end
end

w = WorkerSupervisor.new(10, 20)
w.run
