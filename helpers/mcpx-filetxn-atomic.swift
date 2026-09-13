import Foundation
import CryptoKit
import Darwin

func jsonPrint(_ obj: [String: Any]) {
    let data = try! JSONSerialization.data(withJSONObject: obj, options: [.sortedKeys])
    print(String(decoding: data, as: UTF8.self))
}
func fail(_ msg: String, _ code: Int32) -> Never { fputs(msg + "\n", stderr); exit(code) }

func sha256File(_ path: String) throws -> String {
    let fh = try FileHandle(forReadingFrom: URL(fileURLWithPath: path))
    defer { try? fh.close() }
    var h = SHA256()
    while true {
        let d = try fh.read(upToCount: 1024 * 1024) ?? Data()
        if d.isEmpty { break }
        h.update(data: d)
    }
    return h.finalize().map { String(format: "%02x", $0) }.joined()
}
func exclusiveRename(_ source: String, _ target: String) -> Int32 {
    source.withCString { s in target.withCString { t in renameatx_np(AT_FDCWD, s, AT_FDCWD, t, UInt32(RENAME_EXCL)) } }
}
func swapRename(_ source: String, _ target: String) -> Int32 {
    source.withCString { s in target.withCString { t in renameatx_np(AT_FDCWD, s, AT_FDCWD, t, UInt32(RENAME_SWAP)) } }
}
func plainRename(_ source: String, _ target: String) -> Int32 {
    source.withCString { s in target.withCString { t in rename(s, t) } }
}
func unlinkPath(_ path: String) -> Int32 { path.withCString { unlink($0) } }
func fsyncPath(_ path: String) {
    let fd = open(path, O_RDONLY)
    if fd >= 0 { _ = fsync(fd); close(fd) }
}
func copyMetadata(_ from: String, _ to: String) throws {
    let flags = copyfile_flags_t(COPYFILE_METADATA | COPYFILE_NOFOLLOW)
    let rc = from.withCString { f in to.withCString { t in copyfile(f, t, nil, flags) } }
    if rc != 0 { throw NSError(domain:"mcpx-filetxn", code:Int(errno), userInfo:[NSLocalizedDescriptionKey:"copyfile metadata errno=\(errno): \(String(cString:strerror(errno)))"]) }
    // A content replacement should have a fresh modification time even though permissions/ACL/xattrs/flags are inherited.
    try FileManager.default.setAttributes([.modificationDate: Date()], ofItemAtPath: to)
    fsyncPath(to)
}

let a = CommandLine.arguments
if a.count < 4 { fail("usage: mcpx-filetxn-atomic <create|replace|delete> <target> <expected-or-dash> [replacement]", 64) }
let op = a[1], targetPath = a[2]
let expected = a[3] == "-" ? nil : a[3]

if op == "create" {
    if a.count != 5 { fail("create requires replacement path",64) }
    let replacement = a[4]
    let rc = exclusiveRename(replacement,targetPath)
    if rc == 0 {
        do { let final = try sha256File(targetPath); let bytes = (try FileManager.default.attributesOfItem(atPath:targetPath)[.size] as? NSNumber)?.intValue ?? 0; jsonPrint(["status":"created","new_sha256":final,"bytes":bytes]); exit(0) }
        catch { fail("create_verify_error: \(error)",5) }
    }
    if errno == EEXIST { jsonPrint(["status":"conflict","reason":"target_exists"]); exit(0) }
    fail("exclusive_create_error errno=\(errno): \(String(cString:strerror(errno)))",2)
}
if op != "replace" && op != "delete" { fail("unknown operation \(op)",64) }
if expected == nil { fail("expected sha256 required for \(op)",64) }
if op == "replace" && a.count != 5 { fail("replace requires replacement path",64) }

do {
    let actual = try sha256File(targetPath)
    guard actual == expected! else { jsonPrint(["status":"conflict","reason":"version_mismatch","actual_sha256":actual,"expected_sha256":expected!]); exit(0) }
    if op == "replace" {
        let replacement = a[4]
        do {
            try copyMetadata(targetPath,replacement)
        } catch {
            // A concurrent publisher may have swapped/replaced the target while metadata was being read.
            // Convert that race into a normal CAS conflict when the content version changed.
            if let now = try? sha256File(targetPath), now != expected! {
                jsonPrint(["status":"conflict","reason":"version_mismatch_during_metadata_copy","actual_sha256":now,"expected_sha256":expected!])
                exit(0)
            }
            throw error
        }
        // Recheck immediately before publication so a concurrent external edit is not knowingly overwritten.
        let actual2 = try sha256File(targetPath)
        guard actual2 == expected! else { jsonPrint(["status":"conflict","reason":"version_mismatch","actual_sha256":actual2,"expected_sha256":expected!]); exit(0) }
        let rc = swapRename(replacement,targetPath)
        if rc != 0 {
            if errno == ENOTSUP || errno == EINVAL {
                jsonPrint(["status":"error","reason":"atomic_swap_unsupported","errno":Int(errno)])
                exit(0)
            }
            fail("atomic_replace_error errno=\(errno): \(String(cString:strerror(errno)))",6)
        }
        // replacement now names the exact target object that existed at the atomic publication instant.
        // Validate it *after* the swap. If an external writer changed the target between our precheck
        // and the swap, restore that external version atomically and report a CAS conflict.
        let displacedSha = try sha256File(replacement)
        guard displacedSha == expected! else {
            let rollback = swapRename(replacement,targetPath)
            if rollback != 0 { fail("cas_rollback_failed errno=\(errno): \(String(cString:strerror(errno)))",8) }
            jsonPrint(["status":"conflict","reason":"version_changed_during_publish","actual_sha256":displacedSha,"expected_sha256":expected!])
            exit(0)
        }
        _ = unlinkPath(replacement) // validated old target after the atomic swap
        let final = try sha256File(targetPath)
        let bytes = (try FileManager.default.attributesOfItem(atPath:targetPath)[.size] as? NSNumber)?.intValue ?? 0
        jsonPrint(["status":"written","old_sha256":displacedSha,"new_sha256":final,"bytes":bytes,"publish_method":"swap"])
    } else {
        let actual2 = try sha256File(targetPath)
        guard actual2 == expected! else { jsonPrint(["status":"conflict","reason":"version_mismatch","actual_sha256":actual2,"expected_sha256":expected!]); exit(0) }
        if unlinkPath(targetPath) != 0 { fail("delete_error errno=\(errno): \(String(cString:strerror(errno)))",7) }
        jsonPrint(["status":"deleted","old_sha256":actual])
    }
} catch { fail("operation_error: \(error)",4) }
