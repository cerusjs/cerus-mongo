module.exports = cerus => {
	return new (require("./lib/mongo"))(cerus);
}