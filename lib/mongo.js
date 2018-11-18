const MongoClient = require('mongodb').MongoClient;

class mongo {
	constructor(cerus) {
		this._cerus = cerus;
		this._columns_map = new Map();
	}

	_url() {
		let url = "mongodb://";

		if(typeof this._opts.username === "string" && typeof this._opts.password === "string") {
			url += `${this._opts.username}:${this._opts.password}@`;
		}

		url += this._opts.domain;

		if(typeof this._opts.port === "number") {
			url += `:${this._opts.port}`;
		}

		if(typeof this._opts.database === "string") {
			url += `/${this._opts.database}`;
		}

		return url;
	}

	connect(opts = {}) {
		this._opts = Object.assign({}, {
			domain: "localhost",
			ssl: false
		}, opts);

		let connectionOpts = {
			ssl: this._opts.ssl,
			useNewUrlParser: true
		}

		this._opts.url = this._url();

		return this._cerus.promise(MongoClient.connect(this._opts.url, connectionOpts))
			.then(client => {
				this._client = client;

				return this;
			}, {passthrough: true});
	}

	database(name) {
		return new database(name, this);
	}

	close() {
		return this._client.close();
	}

	_columns(table_name, database_name) {
		let name = `${database_name}#${table_name}`;

		if(this._columns_map.has(name)) {
			return this._columns_map.get(name);
		}

		return this._columns_map.set(name, {}).get(name);
	}
}

class database {
	constructor(name, mongo) {
		this._name = name;
		this._mongo = mongo;
		this._cerus = mongo._cerus;
		this._db = mongo._client.db(name);
	}

	close() {
		return this._cerus.promise(this._mongo.close());
	}

	clone(newdb) {
		return this._cerus.promise(this._db.admin().command({"copydb": 1, fromdb: name, todb: newdb}));
	}

	drop() {
		return this._cerus.promise(this._db.dropDatabase());
	}

	table(name) {
		return new table(name, this);
	}

	name() {
		return this._name;
	}
}

class table {
	constructor(name, database) {
		this._name = name;
		this._database = database;
		this._cerus = database._cerus;
		this._table = database._db.collection(name);
		this._columns = new columns(database._mongo._columns(name, database._name), this._cerus);
	}

	close() {
		return this._database.close()
			.then(() => this, {passthrough: true});
	}

	clear() {
		return this._cerus.promise(this._table.deleteMany({}))
			.then(() => this, {passthrough: true});
	}

	drop() {
		return this._cerus.promise(this._table.drop())
			.then(() => this, {passthrough: true});
	}

	find(query) {
		let _query = this._columns._filter(query);
		let cursor = this._table.find(_query);
		let array;

		return this._cerus.promise(cursor.toArray())
			.then(data => {
				array = data;
				return this._cerus.promise(cursor.close());
			})
			.then(() => this._cerus.promise().event("success", array, this), {passthrough: true});
	}

	count(query) {
		let _query = this._columns._filter(query);

		return this._cerus.promise(this._table.countDocuments(_query))
			.then(value => this._cerus.promise().event("success", value, this));
	}

	delete(filter) {
		let _filter = this._columns._filter(filter);

		return this._cerus.promise(this._table.deleteMany(_filter))
			.then(() => this, {passthrough: true});
	}

	modify(filter, update) {
		let _filter = this._columns._filter(filter);
		let _update = this._columns._filter(update);

		return this._cerus.promise(this._table.updateMany(_filter, {$set: _update}))
			.then(() => this, {passthrough: true});
	}

	insert(docs) {
		let _docs = [];

		if(!(docs instanceof Array)) {
			docs = [docs];
		}

		docs.forEach(doc => {
			_docs.push(this._columns._filter(doc));
		});

		return this._cerus.promise(this._table.insertMany(_docs))
			.then(() => this, {passthrough: true});
	}

	columns() {
		return this._columns;
	}

	keys() {
		return new keys(this._cerus);
	}
}

class columns {
	constructor(columns, cerus) {
		this._columns = columns;
		this._cerus = cerus;
	}

	add(name) {
		return this._cerus.promise(event => {
			this._columns[name] = {name};

			event("success");
		});
	}

	drop(name) {
		return this._cerus.promise(event => {
			delete this._columns[name];

			event("success");
		});
	}

	modify(name, options) {
		return this._cerus.promise(event => {
			let column = this._columns[name];

			if(options["name"]) {
				columns.name = options["name"];

				delete this._columns[name];
			}

			this._columns[column.name] = column;

			event("success");
		});
	}

	has(name) {
		return this._cerus.promise(event => {
			event("success", this._columns[name] !== undefined);
		});
	}

	get(name) {
		return this._cerus.promise(event => {
			event("success", this._columns[name]);
		});
	}

	list() {
		return this._cerus.promise(event => {
			event("success", Object.assign({}, this._columns));
		});
	}

	clear() {
		return this._cerus.promise(event => {
			for(const name of this._columns) {
				delete this._columns[name];
			}

			event("success");
		});
	}

	_filter(query) {
		if(Object.keys(this._columns).length === 0 || Object.keys(query).length === 0) {
			return query;
		}

		let correct_query = {};

		Object.keys(this._columns).forEach(key => correct_query[key] = query[key] || null);

		return correct_query;
	}
}

class keys {
	constructor(cerus) {
		this._cerus = cerus;
	}

	primary() {
		return new primary_keys(this._cerus);
	}

	secondary() {
		return new secondary_keys(this._cerus);
	}
}

class primary_keys {
	constructor(cerus) {
		this._cerus = cerus;
	}

	create() {
		return this._cerus.promise();
	}

	drop() {
		return this._cerus.promise();
	}
}

class secondary_keys {
	constructor(cerus) {
		this._cerus = cerus;
	}

	create() {
		return this._cerus.promise();
	}

	has() {
		return this._cerus.promise();
	}

	drop() {
		return this._cerus.promise();
	}

	list() {
		return this._cerus.promise();
	}

	clear() {
		return this._cerus.promise();
	}
}

module.exports = mongo;