import Database from "better-sqlite3";
import fs from "fs/promises";
import readline from "readline/promises";
import path from "path";
import { Dir } from "fs";

function isSQLiteFile(fileBuffer: Buffer): boolean {
  const expectedHeader = Buffer.from("53514C69746520666F726D6174203300", "hex"); // "SQLite format 3\0"
  const magicHeader = fileBuffer.subarray(4096, 4096 + 16);
  return magicHeader.equals(expectedHeader);
}

function getFileName(fileBuffer: Buffer): string {
  // File name is given in first 100 bytes
  const first1000 = fileBuffer.subarray(0, 100);
  const name = first1000.toString("utf-8").trim().slice(1).replace(/\x00/g, "");

  return name;
}

const r1 = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const directoryPath = path.resolve(
  await r1.question("Enter path to directory: ")
);
r1.close();

type RepositoryName = string;

const repositoryToPositionFiles = new Map<
  RepositoryName,
  Array<{ path: string; positionName: string }>
>();

const parentDir = await fs.opendir(directoryPath);

const traverse = async (dir: Dir, inheritedPositionName?: string) => {
  let positionName = inheritedPositionName;

  for await (const dirEnt of dir) {
    if (dirEnt.name.includes("repositories")) {
      positionName = dirEnt.name.split("-repositories")[0].trim();
    }

    const completePath = path.join(dirEnt.parentPath, dirEnt.name);

    if (dirEnt.isDirectory()) {
      const nestedDir = await fs.opendir(completePath);
      await traverse(nestedDir, positionName);
    } else {
      const fileBuffer = await fs.readFile(completePath);
      if (isSQLiteFile(fileBuffer)) {
        const fileName = getFileName(fileBuffer);
        const newCompletePath = completePath.replace(dirEnt.name, fileName);

        const repositoryPositionFile = repositoryToPositionFiles.get(fileName);
        if (repositoryPositionFile) {
          repositoryPositionFile.push({
            path: `${newCompletePath}.sqlite`,
            positionName,
          });
        } else {
          repositoryToPositionFiles.set(fileName, [
            { path: `${newCompletePath}.sqlite`, positionName },
          ]);
        }
      }
    }
  }
};

await traverse(parentDir);

const compareSnapshotStoreDatasQuery = (p1: string, p2: string) => {
  const sqlQuery = `
SELECT
  hex(S1.objectId) as id1,
  hex(S1.objectId) as id2
  hex(S1.objectHash) as hash1,
  hex(S2.objectHash) as hash2,
  json_array_length(S1.data, "$.participants") as participants,
  json_array_length(S2.data, "$.participants") as participants2,
  json_array_length(S1.data, "$.locations") as locations1,
  json_array_length(S2.data, "$.locations") as locations2,
  json_array_length(S1.data, "$.signalingEvents") as sigEvents1,
  json_array_length(S2.data, "$.signalingEvents") as sigEvents2,
  json_array_length(S1.data, "$.callEvents") as callEvents1,
  json_array_length(S2.data, "$.callEvents") as callEvents2,
  json_pretty(S1.data) as data1,
  json_pretty(S2.data) as data2,
FROM
  "${p1}".snapshots as S1,
  "${p2}".snapshots as S2
WHERE
  s1.objectId = s2.objectId AND s1.data != s2.data 
  `;

  return sqlQuery;
};

console.log(repositoryToPositionFiles);

for (const [storename, positionFiles] of repositoryToPositionFiles) {
  for (let i = 0; i < positionFiles.length; i++) {
    for (let j = i + 1; j < positionFiles.length; j++) {
      const p1DbName = `${storename}_${positionFiles[i].positionName}`.replace(
        /[^a-zA-Z0-9_]/g,
        "_"
      );
      const p2DbName = `${storename}_${positionFiles[j].positionName}`.replace(
        /[^a-zA-Z0-9_]/g,
        "_"
      );
      const db = new Database(":memory:");
      db.exec(`ATTACH DATABASE '${positionFiles[i].path} as "${p1DbName}"`);
      db.exec(`ATTACH DATABASE '${positionFiles[j].path} as "${p2DbName}"`);

      const stmt = db.prepare(
        compareSnapshotStoreDatasQuery(p1DbName, p2DbName)
      );

      const results = stmt.all();

      console.dir(results);
    }
  }
}
