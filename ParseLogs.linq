<Query Kind="Statements" />


var regex = @"^\d\d:\d\d:\d\d.\d\d\d\sDEBUG\s-\s([0-9A-F]{8})\:?\s(.+)$";

const string LogFileName = "c:\\Projects\\torr\\torr.log";

var peerLogs = File.ReadAllLines(LogFileName)
	.Select(line => Regex.Match(line, regex))
	.Where(match => match.Success)
	.Select(match => new {
		PeerId = match.Groups[1].ToString(),
		Message = match.Groups[2].ToString()
	})
	.GroupBy(item => item.PeerId);
	
	peerLogs.Single(g => g.Key == "1C5C11EA").Dump();