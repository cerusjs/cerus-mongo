module.exports = function(cerus) {
	var self = {};

	var mongojs = require('mongojs');
	var database_name = "";
	var database = null;

	self.username = null;
	self.password = null;
	self.domain = null;
	self.port = null;

	self.columns = {};

	self.url = function(domain, port, username, password, database) {
		self.username = username || null;
		self.password = password || null;
		self.domain = domain || "localhost";
		self.port = port || 27017;
		database = database || "test";

		var url = "";

		if(self.username != null && self.password != null && self.port != 27017 && self.domain != "localhost") {
			url += self.username + ":" + self.password + "@" + self.domain + ":" + self.port + "/" + database;
		}
		else {
			url += database;
		}

		return url;
	}

	self.connect = function(domain, port, username, password, database) {
		if(database == null || self.database == "test") {
			mongojs(self.url(domain, port, username, password, "test"));
		}
		else {
			return mongojs(self.url(domain, port, username, password, database));
		}
	}

	self.database = function(name) {
		var db = null;
		var self_ = {};

		if(database_name == name) {
			db = database;
		}
		else {
			db = self.connect(self.domain, self.port, self.username, self.password, name);
		}

		db.on('error', function() {
			console.log("There was an error in the database " + name);
		});

		self_.close = function() {
			db.close();
		}

		self_.clone = function(newdb) {
			return cerus.promise(function(event) {
				self.connect(self.domain, self.port, self.username, self.password, "admin")
				.runCommand({"copydb": 1, fromdb: name, todb: newdb}, function(err, res) {
					if(err || !res.ok) {
						event("error");
					}
					else {
						event("success");
					}
				});
			});
		}

		self_.drop = function() {
			return cerus.promise(function(event) {
				db.dropDatabase(function(err, res) {
					if(err) {
						event("error");
					}
					else {
						event("success");
					}
				});
			});
		}

		self_.table = function(name) {
			var self__ = {};
			var table = db.collection(name);
			var col = db + "." + name;

			self.columns[col] = {};

			self__.close = function() {
				console.log("test");
				db.close();
			}

			self__.clear = function() {
				return cerus.promise(function(event) {
					table.remove({}, false, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success");
						}
					});
				});
			}

			self__.drop = function() {
				return cerus.promise(function(event) {
					table.drop(function(err) {
						if(err) {
							event("error");
						}
						else {
							event("success");
						}
					});
				});
			}

			self__.clone = function(new_) {
				return cerus.promise(function(event) {
					self.columns[db + "." + new_] = self.columns[col];

					var table_ = self_.table(new_);

					table.find().forEach(function(err, doc) {
						if(err) {
							event("error");
							return;
						}
						else if(!doc) {
							event("succes");
							return;
						}

						table_.insert(doc, function(err) {
							if(err) {
								event("err");
							}
						});
					});
				});
			}

			self__.find = function(query) {
				return cerus.promise(function(event) {
					var query_ = {};

					for(var key in self.columns[col]) {
						if(query[key] != null) {
							query_[key] = query[key];
						}
					}

					table.find(query_, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success", res);
						}
					});
				});
			}

			self__.count = function(query) {
				return cerus.promise(function(event) {
					var query_ = {};

					for(var key in self.columns[col]) {
						if(query[key] != null) {
							query_[key] = query[key];
						}
					}

					table.count(query_, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success", res);
						}
					});
				});
			}

			self__.delete = function(query) {
				return cerus.promise(function(event) {
					var query_ = {};

					for(var key in self.columns[col]) {
						if(query[key] != null) {
							query_[key] = query[key];
						}
					}

					table.remove(query_, false, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success");
						}
					});
				});
			}

			self__.modify = function(query, change) {
				return cerus.promise(function(event) {
					var query_ = {};
					var update = {};
					var data = {};

					for(var key in self.columns[col]) {
						if(query[key] != null) {
							query_[key] = query[key];
						}
					}

					for(var key in self.columns[col]) {
						if(change[key] != null) {
							data[key] = change[key];
						}
					}

					update["$set"] = data;

					table.update(query_, update, {multi: true}, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success");
						}
					});
				});
			}

			self__.insert = function(row) {
				return cerus.promise(function(event) {
					var data = {};

					for(var key in self.columns[col]) {
						if(row[key] != null) {
							data[key] = row[key];
						}
						else {
							data[key] = null;
						}
					}

					table.insert(data, function(err, res) {
						if(err) {
							event("error");
						}
						else {
							event("success");
						}
					});
				});
			}

			self__.columns = function() {
				var self___ = {};

				self___.add = function(name, datatype, options) {
					self.columns[col][name] = name;
				}

				self___.drop = function(name) {
					delete self.columns[col][name];
				}

				self___.modify = function(name, options) {
					var column = self.columns[col][name];
					var name_ = name;

					if(options["name"] != null) {
						self___.drop(name);
						name_ = options["name"];
					}

					self.columns[col][name_] = column;
				}

				return self___;
			}

			self__.keys = function() {
				var self_ = {};

				self_.primary = function() {
					var self_ = {};

					self_.create = function() {
						return cerus.promise();
					}

					self_.drop = function() {
						return cerus.promise();
					}

					return self_;
				}

				self_.secondary = function() {
					var self_ = {};

					self_.create = function() {
						return cerus.promise();
					}
					
					self_.drop = function() {
						return cerus.promise();
					}
					
					self_.list = function() {
						return cerus.promise();
					}

					return self_;
				}

				return self_;
			}

			return self__;
		}

		return self_;
	}

	return self;
}