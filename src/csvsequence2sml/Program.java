package csvsequence2sml;

import com.stenway.wsv.WsvStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Program {
	
	private static String[] getFiles(String directoryPath, String extension) {
		try {
			List<String> list;
			
			try (Stream<Path> walk = Files.walk(Paths.get(directoryPath))) {
				list = walk
					.filter(p -> !Files.isDirectory(p))
					.map(p -> p.toString())
					.filter(f -> f.toLowerCase().endsWith(extension))
					.collect(Collectors.toList());
			}
			
			return list.toArray(new String[0]);
		} catch (Exception e) {
			throw new IllegalStateException("Could not get files in '"+directoryPath+"'");
		}
	}
	
	private static String readUtf8FileWithoutBom(String filePath) throws IOException {
		byte[] bytes = Files.readAllBytes(Paths.get(filePath));
		String contents = new String(bytes, StandardCharsets.UTF_8);
		return contents;
	}
	
	private static LinkedHashSet<String> readLines(String filePath, boolean windowsLineBreak) throws IOException {
		String contents = readUtf8FileWithoutBom(filePath);
		String lineBreakStr = "\\n";
		if (windowsLineBreak) {
			lineBreakStr = "\\r\\n";
		}
		String[] lines = contents.split(lineBreakStr);
		LinkedHashSet<String> result = new LinkedHashSet<>();
		for (String line : lines) {
			result.add(line);
		}
		return result;
	}
	
	private static void writeWsvLines(Collection<String> csvLines, WsvStreamWriter streamWriter) throws IOException {
		for (String csvLine : csvLines) {
			String[] values = csvLine.split(",");
			streamWriter.writeLine(values);
		}
	}
	
	public static void main(String[] args) {
		try {
			
			String csvsDirectoryPath = "D:\\RKICsvSequenz\\SARS-CoV-2_Infektionen_in_Deutschland-2022-01-12\\robert-koch-institut-SARS-CoV-2_Infektionen_in_Deutschland-a0eecea\\Archiv";
			String outputSmlFilePath = "D:\\RKICsvSequenz\\Sequenz.sml";
			
			String[] filePaths = getFiles(csvsDirectoryPath, ".csv");
			WsvStreamWriter streamWriter = new WsvStreamWriter(outputSmlFilePath);
			String endKeyword = "End";
			streamWriter.writeLine("Sequence");
			
			LinkedHashSet<String> lastLines = null;
			
			for (int i=0; i<filePaths.length; i++) {
				String csvFilePath = filePaths[i];
				String name = Paths.get(csvFilePath).getFileName().toString();
				
				int progress = i*100/(filePaths.length-1);
				System.out.print("Processing '"+name+"' "+progress+"% ");
				
				LinkedHashSet<String> curLinesLoaded = readLines(csvFilePath, true);
				
				System.out.print(".");
				
				streamWriter.writeLine(name);
				if (i == 0) {
					streamWriter.writeLine("Added");
					writeWsvLines(curLinesLoaded, streamWriter);
					streamWriter.writeLine(endKeyword);
				} else {
					LinkedHashSet<String> curLines = (LinkedHashSet<String>)curLinesLoaded.clone();
					ArrayList<String> removedLines = new ArrayList<>();
					for (String lineInLast : lastLines) {
						if (curLines.contains(lineInLast)) {
							curLines.remove(lineInLast);
						} else {
							removedLines.add(lineInLast);
						}
					}
					
					streamWriter.writeLine("Removed");
					writeWsvLines(removedLines, streamWriter);
					streamWriter.writeLine(endKeyword);

					streamWriter.writeLine("Added");
					writeWsvLines(curLines, streamWriter);
					streamWriter.writeLine(endKeyword);
				}
				
				lastLines = curLinesLoaded;
				
				streamWriter.writeLine(endKeyword);
				System.out.println(".");
			}
			streamWriter.writeLine(endKeyword);
			streamWriter.close();
			
			System.out.println("[SUCCESS]");
			
		} catch(Exception e) {
			System.out.println("[ERROR] "+e.getMessage());
		}	
	}
}