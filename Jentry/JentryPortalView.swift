import SwiftUI
import WebKit
import Combine
import UniformTypeIdentifiers
import UIKit

struct JentryPortalView: View {
    @StateObject private var viewModel = JentryPortalViewModel()
    private let refreshInterval: Duration = .seconds(30)

    var body: some View {
        JentryPortalWebView(viewModel: viewModel)
            .ignoresSafeArea()
            .task {
                await viewModel.refresh()
            }
            .task {
                await pollForUpdates()
            }
    }

    private func pollForUpdates() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: refreshInterval)
            guard Task.isCancelled == false else { return }
            await viewModel.refresh()
        }
    }
}

@MainActor
final class JentryPortalViewModel: ObservableObject {
    @Published fileprivate var portalState = JentryPortalState.loading

    private let service: JentryCloudServiceProtocol
    private var dashboardPayload: DashboardPayload?

    init(service: JentryCloudServiceProtocol? = nil) {
        self.service = service ?? LiveJentryCloudService()
    }

    func refresh() async {
        portalState = portalState.withLoading(true)

        do {
            let dashboard = try await service.fetchDashboard()
            dashboardPayload = dashboard
            portalState = JentryPortalState(dashboard: dashboard)
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func retrySubmission(id: UUID) async {
        do {
            _ = try await service.retrySubmission(submissionID: id)
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func markSubmissionReady(id: UUID) async {
        do {
            _ = try await service.markSubmissionReadyForXero(submissionID: id)
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func publishSubmission(id: UUID) async {
        do {
            _ = try await service.publishSubmission(submissionID: id)
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func archiveSubmission(id: UUID) async {
        do {
            _ = try await service.archiveSubmission(submissionID: id)
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func updateInboundEmail(_ email: String) async {
        guard let workspaceID = dashboardPayload?.workspace.id else { return }

        do {
            let workspace = try await service.updateWorkspaceInboundEmail(workspaceID: workspaceID, email: email)
            if var dashboardPayload {
                dashboardPayload = DashboardPayload(workspace: workspace, submissions: dashboardPayload.submissions)
                self.dashboardPayload = dashboardPayload
                portalState = JentryPortalState(dashboard: dashboardPayload)
            }
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func updateReviewMetadata(submissionID: UUID, nominalCode: String, isAutoCategorised: Bool) async {
        do {
            _ = try await service.updateSubmissionReviewMetadata(
                submissionID: submissionID,
                nominalCode: nominalCode,
                isAutoCategorised: isAutoCategorised
            )
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    func uploadDocument(from url: URL) async {
        do {
            let payload = try DocumentUploadPayload.fileURL(url)
            _ = try await service.uploadDocument(payload)
            await refresh()
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }

    fileprivate func exportSubmissions(scope: PortalExportScope) {
        guard let dashboardPayload else {
            portalState = portalState.withError("No dashboard data is available to export.")
            return
        }

        let allRows = dashboardPayload.submissions
            .map { PortalSubmission(record: $0, clientName: dashboardPayload.workspace.companyName) }
            .sorted { $0.sortDate > $1.sortDate }

        let rows: [PortalSubmission]
        let exportName: String

        switch scope {
        case .allDocuments:
            rows = allRows
            exportName = "all-clients"
        case let .clientWorkspace(workspaceID):
            guard dashboardPayload.workspace.id.uuidString == workspaceID else {
                portalState = portalState.withError("The selected client workspace could not be exported.")
                return
            }
            rows = allRows.filter { $0.clientID == workspaceID }
            exportName = dashboardPayload.workspace.companyName
        }

        let csv = PortalCSVExporter.makeCSV(
            exportLabel: exportName,
            submissions: rows
        )

        do {
            let fileURL = try PortalCSVExporter.writeCSV(
                csv,
                exportLabel: exportName
            )
            PortalSharePresenter.present(fileURL: fileURL)
        } catch {
            portalState = portalState.withError(error.localizedDescription)
        }
    }
}

private struct JentryPortalWebView: UIViewRepresentable {
    @ObservedObject var viewModel: JentryPortalViewModel

    func makeCoordinator() -> Coordinator {
        Coordinator(
            onAction: { action in
                switch action {
                case .refresh:
                    Task { @MainActor in
                        await viewModel.refresh()
                    }
                case let .retrySubmission(id):
                    Task { @MainActor in
                        await viewModel.retrySubmission(id: id)
                    }
                case let .markSubmissionReady(id):
                    Task { @MainActor in
                        await viewModel.markSubmissionReady(id: id)
                    }
                case let .publishSubmission(id):
                    Task { @MainActor in
                        await viewModel.publishSubmission(id: id)
                    }
                case let .archiveSubmission(id):
                    Task { @MainActor in
                        await viewModel.archiveSubmission(id: id)
                    }
                case let .updateInboundEmail(email):
                    Task { @MainActor in
                        await viewModel.updateInboundEmail(email)
                    }
                case let .updateReviewMetadata(id, nominalCode, isAutoCategorised):
                    Task { @MainActor in
                        await viewModel.updateReviewMetadata(
                            submissionID: id,
                            nominalCode: nominalCode,
                            isAutoCategorised: isAutoCategorised
                        )
                    }
                case let .export(scope):
                    viewModel.exportSubmissions(scope: scope)
                }
            }
        ,
            onUpload: { url in
                Task { @MainActor in
                    await viewModel.uploadDocument(from: url)
                }
            }
        )
    }

    func makeUIView(context: Context) -> WKWebView {
        let userContentController = WKUserContentController()
        userContentController.add(context.coordinator, name: "jentryPortal")

        let configuration = WKWebViewConfiguration()
        configuration.userContentController = userContentController

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.scrollView.contentInsetAdjustmentBehavior = .never
        webView.allowsBackForwardNavigationGestures = true
        webView.backgroundColor = .clear
        webView.isOpaque = false

        context.coordinator.webView = webView
        context.coordinator.loadPortal()

        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {
        context.coordinator.latestState = viewModel.portalState
        context.coordinator.push(state: viewModel.portalState)
    }

    final class Coordinator: NSObject, WKNavigationDelegate, WKScriptMessageHandler, UIDocumentPickerDelegate {
        let onAction: (PortalAction) -> Void
        let onUpload: (URL) -> Void
        weak var webView: WKWebView?
        var latestState = JentryPortalState.loading
        private var didFinishNavigation = false

        init(onAction: @escaping (PortalAction) -> Void, onUpload: @escaping (URL) -> Void) {
            self.onAction = onAction
            self.onUpload = onUpload
        }

        func loadPortal() {
            guard let webView,
                  let fileURL = Bundle.main.url(forResource: "JentryPortal", withExtension: "html") else {
                webView?.loadHTMLString("<html><body><p>JentryPortal.html could not be found in the app bundle.</p></body></html>", baseURL: nil)
                return
            }

            webView.loadFileURL(fileURL, allowingReadAccessTo: fileURL.deletingLastPathComponent())
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            didFinishNavigation = true
            push(state: latestState)
        }

        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            guard message.name == "jentryPortal",
                  let body = message.body as? [String: Any],
                  let action = body["action"] as? String else {
                return
            }

            switch action {
            case "refresh":
                onAction(.refresh)
            case "retrySubmission":
                guard let id = parseUUID(body["submissionID"]) else { return }
                onAction(.retrySubmission(id))
            case "markSubmissionReady":
                guard let id = parseUUID(body["submissionID"]) else { return }
                onAction(.markSubmissionReady(id))
            case "publishSubmission":
                guard let id = parseUUID(body["submissionID"]) else { return }
                onAction(.publishSubmission(id))
            case "archiveSubmission":
                guard let id = parseUUID(body["submissionID"]) else { return }
                onAction(.archiveSubmission(id))
            case "updateInboundEmail":
                guard let email = body["email"] as? String else { return }
                onAction(.updateInboundEmail(email))
            case "updateReviewMetadata":
                guard let id = parseUUID(body["submissionID"]),
                      let nominalCode = body["nominalCode"] as? String,
                      let isAutoCategorised = body["isAutoCategorised"] as? Bool else { return }
                onAction(.updateReviewMetadata(id, nominalCode: nominalCode, isAutoCategorised: isAutoCategorised))
            case "requestUpload":
                presentDocumentPicker()
            case "exportClient":
                guard let workspaceID = body["workspaceID"] as? String else { return }
                onAction(.export(scope: .clientWorkspace(workspaceID: workspaceID)))
            case "exportAll":
                onAction(.export(scope: .allDocuments))
            default:
                break
            }
        }

        func presentDocumentPicker() {
            let supportedTypes: [UTType] = [.pdf, .image]
            let picker = UIDocumentPickerViewController(forOpeningContentTypes: supportedTypes, asCopy: true)
            picker.delegate = self
            picker.allowsMultipleSelection = false
            topViewController()?.present(picker, animated: true)
        }

        func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
            guard let url = urls.first else { return }
            onUpload(url)
        }

        private func topViewController() -> UIViewController? {
            guard let scene = UIApplication.shared.connectedScenes
                .compactMap({ $0 as? UIWindowScene })
                .first,
                  let rootViewController = scene.windows.first(where: \.isKeyWindow)?.rootViewController else {
                return nil
            }

            var top = rootViewController
            while let presented = top.presentedViewController {
                top = presented
            }
            return top
        }

        func push(state: JentryPortalState) {
            guard didFinishNavigation, let webView else { return }
            guard let data = try? JSONEncoder.jentry.encode(state),
                  let stateJSONString = String(data: data, encoding: .utf8) else {
                return
            }

            let script = "window.JentryPortal && window.JentryPortal.setState(\(stateJSONString));"
            webView.evaluateJavaScript(script)
        }

        private func parseUUID(_ value: Any?) -> UUID? {
            guard let rawValue = value as? String else { return nil }
            return UUID(uuidString: rawValue)
        }
    }
}

fileprivate struct JentryPortalState: Codable {
    let isLoading: Bool
    let errorMessage: String?
    let workspace: PortalWorkspace
    let stats: PortalStats
    let statuses: [PortalStatusItem]
    let submissions: [PortalSubmission]
    let clients: [PortalClient]
    let users: [PortalUser]
    let lastUpdated: String

    static let loading = JentryPortalState(
        isLoading: true,
        errorMessage: nil,
        workspace: PortalWorkspace.placeholder,
        stats: PortalStats(totalSubmissions: 0, appUploads: 0, inboundEmails: 0, needsReview: 0),
        statuses: [],
        submissions: [],
        clients: [],
        users: [],
        lastUpdated: Date.now.formatted(date: .abbreviated, time: .shortened)
    )

    init(
        isLoading: Bool,
        errorMessage: String?,
        workspace: PortalWorkspace,
        stats: PortalStats,
        statuses: [PortalStatusItem],
        submissions: [PortalSubmission],
        clients: [PortalClient],
        users: [PortalUser],
        lastUpdated: String
    ) {
        self.isLoading = isLoading
        self.errorMessage = errorMessage
        self.workspace = workspace
        self.stats = stats
        self.statuses = statuses
        self.submissions = submissions
        self.clients = clients
        self.users = users
        self.lastUpdated = lastUpdated
    }

    init(dashboard: DashboardPayload) {
        let submissions = dashboard.submissions.map { PortalSubmission(record: $0, clientName: dashboard.workspace.companyName) }
            .sorted { $0.sortDate > $1.sortDate }

        let reviewCount = submissions.filter { $0.processing == "Needs Review" || $0.xero == "Failed" }.count
        let appUploadCount = submissions.filter { $0.source == "App Upload" }.count
        let inboundEmailCount = submissions.filter { $0.source == "Inbound Email" }.count
        let inboxCount = submissions.filter(\.isInbox).count
        let archiveCount = submissions.filter(\.isArchived).count

        self.isLoading = false
        self.errorMessage = nil
        self.workspace = PortalWorkspace(
            id: dashboard.workspace.id.uuidString,
            companyName: dashboard.workspace.companyName,
            inboundEmail: dashboard.workspace.jentryInboundEmail,
            xeroStatus: dashboard.workspace.xeroConnectionStatus.displayName,
            xeroTenantID: dashboard.workspace.xeroTenantID ?? "Not connected"
        )
        self.stats = PortalStats(
            totalSubmissions: submissions.count,
            appUploads: appUploadCount,
            inboundEmails: inboundEmailCount,
            needsReview: reviewCount
        )
        self.statuses = [
            PortalStatusItem(label: "API Connected", value: "Live"),
            PortalStatusItem(label: "Inbound Email", value: dashboard.workspace.jentryInboundEmail),
            PortalStatusItem(label: "Xero", value: dashboard.workspace.xeroConnectionStatus.displayName),
            PortalStatusItem(label: "Queue", value: reviewCount == 0 ? "Healthy" : "\(reviewCount) Review")
        ]
        self.submissions = submissions
        self.clients = [PortalClient(
            id: dashboard.workspace.id.uuidString,
            name: dashboard.workspace.companyName,
            email: dashboard.workspace.jentryInboundEmail,
            docs: submissions.count,
            review: inboxCount,
            status: dashboard.workspace.xeroConnectionStatus == .connected ? "Active" : "Attention",
            lastActivity: submissions.first?.received ?? "No submissions yet",
            readyToPublish: submissions.filter(\.canPublish).count,
            published: archiveCount,
            lastSupplier: submissions.first?.supplier ?? dashboard.workspace.companyName,
            inboxCount: inboxCount,
            archiveCount: archiveCount
        )]
        self.users = PortalUser.makeUsers(from: dashboard.submissions)
        self.lastUpdated = Date.now.formatted(date: .abbreviated, time: .shortened)
    }

    func withLoading(_ loading: Bool) -> JentryPortalState {
        JentryPortalState(
            isLoading: loading,
            errorMessage: errorMessage,
            workspace: workspace,
            stats: stats,
            statuses: statuses,
            submissions: submissions,
            clients: clients,
            users: users,
            lastUpdated: lastUpdated
        )
    }

    func withError(_ error: String) -> JentryPortalState {
        JentryPortalState(
            isLoading: false,
            errorMessage: error,
            workspace: workspace,
            stats: stats,
            statuses: statuses,
            submissions: submissions,
            clients: clients,
            users: users,
            lastUpdated: lastUpdated
        )
    }
}

private struct PortalWorkspace: Codable {
    let id: String
    let companyName: String
    let inboundEmail: String
    let xeroStatus: String
    let xeroTenantID: String

    static let placeholder = PortalWorkspace(
        id: "",
        companyName: "Loading workspace",
        inboundEmail: "Loading",
        xeroStatus: "Connecting",
        xeroTenantID: "Loading"
    )
}

private struct PortalStats: Codable {
    let totalSubmissions: Int
    let appUploads: Int
    let inboundEmails: Int
    let needsReview: Int
}

private struct PortalStatusItem: Codable {
    let label: String
    let value: String
}

private struct PortalSubmission: Codable {
    let id: String
    let clientID: String
    let client: String
    let doc: String
    let supplier: String
    let documentType: String
    let documentDate: String
    let totalAmount: String
    let taxAmount: String
    let taxRate: String
    let nominalCode: String
    let isAutoCategorised: Bool
    let matchStatus: String
    let publishState: String
    let canPublish: Bool
    let isInbox: Bool
    let isArchived: Bool
    let source: String
    let by: String
    let email: String
    let processing: String
    let xero: String
    let received: String
    let issue: String
    let canRetry: Bool
    let canMarkReady: Bool
    let sortDate: Date

    init(record: SubmissionRecord, clientName: String) {
        self.id = record.id.uuidString
        self.clientID = record.workspaceID.uuidString
        self.client = clientName
        self.doc = record.originalFilename
        self.supplier = record.extractedData?.supplierName ?? clientName
        self.documentType = Self.documentType(for: record)
        self.documentDate = record.extractedData?.invoiceDate ?? record.createdAt.formatted(date: .abbreviated, time: .omitted)
        self.totalAmount = Self.amountText(record.extractedData?.grossAmount, currency: record.extractedData?.currency)
        self.taxAmount = Self.amountText(record.extractedData?.vatAmount, currency: record.extractedData?.currency)
        self.taxRate = Self.taxRateText(for: record.extractedData)
        self.nominalCode = record.nominalCode ?? Self.defaultNominalCode(for: record)
        self.isAutoCategorised = record.isAutoCategorised ?? true
        self.matchStatus = Self.matchStatus(for: record)
        self.publishState = Self.publishState(for: record)
        self.canPublish = record.isArchived == false
        self.isInbox = record.isInbox
        self.isArchived = record.isArchived
        self.source = record.source.displayName.replacingOccurrences(of: "upload", with: "Upload")
        self.by = record.emailFrom ?? record.emailSubject ?? "App user"
        self.email = record.emailFrom ?? "—"
        self.processing = record.status.displayName
        self.xero = record.xeroStatus?.replacingOccurrences(of: "_", with: " ").capitalized ?? Self.fallbackXeroStatus(for: record.status)
        self.received = record.createdAt.formatted(date: .abbreviated, time: .shortened)
        self.issue = record.errorMessage ?? Self.defaultIssue(for: record.status)
        self.canRetry = record.status == .failed || record.status == .needsReview
        self.canMarkReady = record.status == .needsReview
        self.sortDate = record.createdAt
    }

    static func fallbackXeroStatus(for status: DocumentStatus) -> String {
        switch status {
        case .readyForXero:
            return "Ready"
        case .exported:
            return "Exported"
        case .failed:
            return "Failed"
        default:
            return "Not Sent"
        }
    }

    static func defaultIssue(for status: DocumentStatus) -> String {
        switch status {
        case .needsReview:
            return "Manual validation required"
        case .failed:
            return "Processing failed"
        default:
            return "No issue"
        }
    }

    static func documentType(for record: SubmissionRecord) -> String {
        let lowercaseName = record.originalFilename.lowercased()

        if lowercaseName.contains("receipt") {
            return "Receipt"
        }

        if lowercaseName.contains("invoice") {
            return "Invoice"
        }

        return record.source == .inboundEmail ? "Email Document" : "Uploaded Document"
    }

    static func amountText(_ amount: Decimal?, currency: String?) -> String {
        guard let amount else { return "Pending" }

        return amount.formatted(.currency(code: currency ?? "GBP"))
    }

    static func taxRateText(for extractedData: ExtractedDocumentData?) -> String {
        guard let extractedData,
              extractedData.netAmount != 0 else {
            return "Unknown"
        }

        let vatDecimal = NSDecimalNumber(decimal: extractedData.vatAmount).doubleValue
        let netDecimal = NSDecimalNumber(decimal: extractedData.netAmount).doubleValue
        let rate = vatDecimal / netDecimal

        return rate.formatted(.percent.precision(.fractionLength(0)))
    }

    static func defaultNominalCode(for record: SubmissionRecord) -> String {
        let category = record.extractedData?.category.lowercased() ?? ""
        let supplier = record.extractedData?.supplierName.lowercased() ?? ""

        if category.contains("travel") {
            return "400 - Travel & Mileage"
        }

        if category.contains("advert") || category.contains("marketing") {
            return "400 - Advertising & Marketing"
        }

        if category.contains("office") {
            return "720 - Office Expenses"
        }

        if supplier.contains("meta") {
            return "400 - Advertising & Marketing"
        }

        if supplier.contains("pet") || supplier.contains("food") {
            return "310 - Cost of Goods Sold"
        }

        return "310 - Cost of Goods Sold"
    }

    static func matchStatus(for record: SubmissionRecord) -> String {
        if record.isArchived {
            return "Archived"
        }

        switch record.status {
        case .exported:
            return "Published to Xero"
        case .readyForXero:
            return "Ready to Publish"
        case .needsReview:
            return "Needs Manual Check"
        case .failed:
            return "Requires Retry"
        default:
            return "Awaiting Review"
        }
    }

    static func publishState(for record: SubmissionRecord) -> String {
        if record.archivedAt != nil && record.status != .exported {
            return "Archived"
        }

        switch record.status {
        case .exported:
            return "Published"
        case .readyForXero:
            return "Ready"
        case .needsReview:
            return "Review"
        case .failed:
            return "Blocked"
        default:
            return "Draft"
        }
    }
}

private struct PortalClient: Codable {
    let id: String
    let name: String
    let email: String
    let docs: Int
    let review: Int
    let status: String
    let lastActivity: String
    let readyToPublish: Int
    let published: Int
    let lastSupplier: String
    let inboxCount: Int
    let archiveCount: Int
}

private struct PortalUser: Codable {
    let id: String
    let name: String
    let email: String
    let role: String
    let status: String
    let lastActive: String

    static func makeUsers(from submissions: [SubmissionRecord]) -> [PortalUser] {
        let usersByEmail = Dictionary(grouping: submissions.compactMap { record -> (String, Date)? in
            guard let email = record.emailFrom, email.isEmpty == false else { return nil }
            return (email, record.createdAt)
        }, by: { $0.0 })

        return usersByEmail.map { email, entries in
            PortalUser(
                id: email,
                name: email.components(separatedBy: "@").first?.replacingOccurrences(of: ".", with: " ").capitalized ?? email,
                email: email,
                role: "Observed Sender",
                status: "Active",
                lastActive: entries.map(\.1).max()?.formatted(date: .abbreviated, time: .shortened) ?? "Unknown"
            )
        }
        .sorted { $0.email < $1.email }
    }
}

private enum PortalAction {
    case refresh
    case retrySubmission(UUID)
    case markSubmissionReady(UUID)
    case publishSubmission(UUID)
    case archiveSubmission(UUID)
    case updateInboundEmail(String)
    case updateReviewMetadata(UUID, nominalCode: String, isAutoCategorised: Bool)
    case export(scope: PortalExportScope)
}

private enum PortalExportScope {
    case clientWorkspace(workspaceID: String)
    case allDocuments
}

private enum PortalCSVExporter {
    nonisolated static func makeCSV(exportLabel: String, submissions: [PortalSubmission]) -> String {
        let header = [
            "Client",
            "Document",
            "Supplier",
            "Document Date",
            "Total Amount",
            "Xero Account Code",
            "Publish Status",
            "Xero Status"
        ]

        let body = submissions.map { submission in
            [
                submission.client,
                submission.doc,
                submission.supplier,
                submission.documentDate,
                submission.totalAmount,
                submission.nominalCode,
                submission.publishState,
                submission.xero
            ]
            .map(csvField)
            .joined(separator: ",")
        }

        return ([header.map(csvField).joined(separator: ",")] + body).joined(separator: "\n")
    }

    nonisolated static func writeCSV(_ csv: String, exportLabel: String) throws -> URL {
        let fileName = sanitizedFileName(exportLabel) + "-jentry-export.csv"
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(fileName)
        try csv.write(to: destination, atomically: true, encoding: .utf8)
        return destination
    }

    nonisolated private static func sanitizedFileName(_ value: String) -> String {
        let slug = value
            .lowercased()
            .replacingOccurrences(of: "[^a-z0-9]+", with: "-", options: .regularExpression)
            .trimmingCharacters(in: CharacterSet(charactersIn: "-"))

        return slug.isEmpty ? "client" : slug
    }

    nonisolated private static func csvField(_ value: String) -> String {
        let escaped = value.replacingOccurrences(of: "\"", with: "\"\"")
        return "\"\(escaped)\""
    }
}

@MainActor
private enum PortalSharePresenter {
    static func present(fileURL: URL) {
        guard let topViewController = topViewController() else { return }

        let activityViewController = UIActivityViewController(activityItems: [fileURL], applicationActivities: nil)

        if let popoverController = activityViewController.popoverPresentationController {
            popoverController.sourceView = topViewController.view
            popoverController.sourceRect = CGRect(
                x: topViewController.view.bounds.midX,
                y: topViewController.view.bounds.midY,
                width: 1,
                height: 1
            )
        }

        topViewController.present(activityViewController, animated: true)
    }

    private static func topViewController() -> UIViewController? {
        guard let scene = UIApplication.shared.connectedScenes
            .compactMap({ $0 as? UIWindowScene })
            .first,
              let rootViewController = scene.windows.first(where: \.isKeyWindow)?.rootViewController else {
            return nil
        }

        var top = rootViewController
        while let presented = top.presentedViewController {
            top = presented
        }
        return top
    }
}

private extension JSONEncoder {
    static let jentry: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }()
}

#Preview {
    JentryPortalView()
}
