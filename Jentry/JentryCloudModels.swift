import Foundation
import UniformTypeIdentifiers

nonisolated struct Workspace: Codable, Equatable {
    let id: UUID
    let companyName: String
    let jentryInboundEmail: String
    let xeroTenantID: String?
    let xeroConnectionStatus: XeroConnectionStatus
}

nonisolated enum XeroConnectionStatus: String, Codable, Equatable {
    case connected
    case disconnected
    case reconnectRequired = "reconnect_required"

    var displayName: String {
        switch self {
        case .connected:
            return "Connected"
        case .disconnected:
            return "Disconnected"
        case .reconnectRequired:
            return "Reconnect required"
        }
    }
}

nonisolated enum DocumentSource: String, Codable, Equatable {
    case appUpload = "app_upload"
    case inboundEmail = "inbound_email"

    var displayName: String {
        switch self {
        case .appUpload:
            return "App upload"
        case .inboundEmail:
            return "Inbound email"
        }
    }
}

nonisolated enum DocumentStatus: String, Codable, Equatable, CaseIterable {
    case received
    case queued
    case processing
    case needsReview = "needs_review"
    case readyForXero = "ready_for_xero"
    case exporting
    case exported
    case failed

    static let dashboardOrder: [DocumentStatus] = [
        .received,
        .queued,
        .processing,
        .needsReview,
        .readyForXero,
        .exporting,
        .exported,
        .failed
    ]

    var displayName: String {
        switch self {
        case .received:
            return "Received"
        case .queued:
            return "Queued"
        case .processing:
            return "Processing"
        case .needsReview:
            return "Needs review"
        case .readyForXero:
            return "Ready for Xero"
        case .exporting:
            return "Exporting"
        case .exported:
            return "In Xero"
        case .failed:
            return "Failed"
        }
    }

    var symbolName: String {
        switch self {
        case .received:
            return "tray.and.arrow.down"
        case .queued:
            return "list.bullet.rectangle"
        case .processing:
            return "gearshape.2"
        case .needsReview:
            return "exclamationmark.bubble"
        case .readyForXero:
            return "checkmark.circle"
        case .exporting:
            return "arrow.up.doc"
        case .exported:
            return "checkmark.seal"
        case .failed:
            return "xmark.octagon"
        }
    }

    var requiresReview: Bool {
        self == .needsReview || self == .failed
    }

    var isArchived: Bool {
        self == .exported
    }

    var isInbox: Bool {
        isArchived == false
    }
}

nonisolated struct ExtractedDocumentData: Codable, Equatable {
    let supplierName: String
    let invoiceNumber: String
    let invoiceDate: String
    let dueDate: String
    let netAmount: Decimal
    let vatAmount: Decimal
    let grossAmount: Decimal
    let currency: String
    let category: String
    let confidence: Double
}

nonisolated struct SubmissionRecord: Identifiable, Codable, Equatable {
    let id: UUID
    let workspaceID: UUID
    let userID: UUID?
    let source: DocumentSource
    let originalFilename: String
    let fileURL: URL?
    let emailFrom: String?
    let emailSubject: String?
    let status: DocumentStatus
    let xeroStatus: String?
    let nominalCode: String?
    let isAutoCategorised: Bool?
    let archivedAt: Date?
    let createdAt: Date
    let extractedData: ExtractedDocumentData?
    let errorMessage: String?

    var isArchived: Bool {
        archivedAt != nil || status == .exported
    }

    var isInbox: Bool {
        isArchived == false
    }
}

nonisolated struct DashboardPayload: Codable, Equatable {
    let workspace: Workspace
    var submissions: [SubmissionRecord]
}

nonisolated struct DocumentUploadPayload {
    let filename: String
    let mimeType: String
    let data: Data
    let source: DocumentSource

    static func fileURL(_ url: URL) throws -> DocumentUploadPayload {
        let shouldStopAccessing = url.startAccessingSecurityScopedResource()
        defer {
            if shouldStopAccessing {
                url.stopAccessingSecurityScopedResource()
            }
        }

        let data = try Data(contentsOf: url)
        let mimeType = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType ?? "application/octet-stream"
        return DocumentUploadPayload(
            filename: url.lastPathComponent,
            mimeType: mimeType,
            data: data,
            source: .appUpload
        )
    }
}

protocol JentryCloudServiceProtocol {
    func fetchDashboard() async throws -> DashboardPayload
    func uploadDocument(_ payload: DocumentUploadPayload) async throws -> SubmissionRecord
    func retrySubmission(submissionID: UUID) async throws -> SubmissionRecord
    func markSubmissionReadyForXero(submissionID: UUID) async throws -> SubmissionRecord
    func publishSubmission(submissionID: UUID) async throws -> SubmissionRecord
    func archiveSubmission(submissionID: UUID) async throws -> SubmissionRecord
    func updateSubmissionReviewMetadata(submissionID: UUID, nominalCode: String, isAutoCategorised: Bool) async throws -> SubmissionRecord
    func updateWorkspaceInboundEmail(workspaceID: UUID, email: String) async throws -> Workspace
}

nonisolated struct JentryServiceConfiguration {
    let apiBaseURL: URL?
    let apiBearerToken: String?

    nonisolated init(apiBaseURL: URL?, apiBearerToken: String?) {
        self.apiBaseURL = apiBaseURL
        self.apiBearerToken = apiBearerToken
    }

    nonisolated init(bundle: Bundle = .main) {
        if let rawURL = bundle.object(forInfoDictionaryKey: "JentryAPIBaseURL") as? String,
           rawURL.isEmpty == false,
           let url = URL(string: rawURL) {
            apiBaseURL = url
        } else if let rawURL = bundle.object(forInfoDictionaryKey: "JENTRYBackendBaseURL") as? String,
           rawURL.isEmpty == false,
           let url = URL(string: rawURL) {
            apiBaseURL = url
        } else {
            apiBaseURL = nil
        }

        if let rawToken = bundle.object(forInfoDictionaryKey: "JentryAPIBearerToken") as? String,
           rawToken.isEmpty == false {
            apiBearerToken = rawToken
        } else if let rawToken = bundle.object(forInfoDictionaryKey: "JENTRYBackendBearerToken") as? String,
                  rawToken.isEmpty == false {
            apiBearerToken = rawToken
        } else {
            apiBearerToken = nil
        }
    }
}

nonisolated enum JentryCloudError: LocalizedError {
    case authenticationRequired
    case invalidResponse

    var errorDescription: String? {
        switch self {
        case .authenticationRequired:
            return "Authentication required. Add a backend bearer token to Info.plist."
        case .invalidResponse:
            return "The server returned an invalid response."
        }
    }
}

actor LiveJentryCloudService: JentryCloudServiceProtocol {
    let configuration: JentryServiceConfiguration
    private let session: URLSession
    private var mockStorage = MockDashboardData.sample

    init(configuration: JentryServiceConfiguration = .init(), session: URLSession = .shared) {
        self.configuration = configuration
        self.session = session
    }

    func fetchDashboard() async throws -> DashboardPayload {
        guard let baseURL = configuration.apiBaseURL else {
            return mockStorage
        }

        let request = try makeRequest(
            url: baseURL.appending(path: "/api/v1/dashboard")
        )
        return try await perform(request, decode: DashboardPayload.self)
    }

    func uploadDocument(_ payload: DocumentUploadPayload) async throws -> SubmissionRecord {
        guard let baseURL = configuration.apiBaseURL else {
            let submission = SubmissionRecord(
                id: UUID(),
                workspaceID: mockStorage.workspace.id,
                userID: nil,
                source: payload.source,
                originalFilename: payload.filename,
                fileURL: nil,
                emailFrom: nil,
                emailSubject: nil,
                status: .queued,
                xeroStatus: "queued",
                nominalCode: nil,
                isAutoCategorised: true,
                archivedAt: nil,
                createdAt: .now,
                extractedData: nil,
                errorMessage: nil
            )
            mockStorage.submissions.insert(submission, at: 0)
            return submission
        }

        let boundary = UUID().uuidString
        var request = try makeRequest(
            url: baseURL.appending(path: "/api/v1/submissions"),
            method: "POST"
        )
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.httpBody = MultipartFormDataBuilder(boundary: boundary)
            .addTextField(named: "source", value: payload.source.rawValue)
            .addFileField(named: "file", filename: payload.filename, mimeType: payload.mimeType, data: payload.data)
            .build()

        return try await perform(request, decode: SubmissionRecord.self)
    }

    func retrySubmission(submissionID: UUID) async throws -> SubmissionRecord {
        try await updateSubmissionStatus(submissionID: submissionID, endpoint: "retry")
    }

    func markSubmissionReadyForXero(submissionID: UUID) async throws -> SubmissionRecord {
        try await updateSubmissionStatus(submissionID: submissionID, endpoint: "mark-ready")
    }

    func publishSubmission(submissionID: UUID) async throws -> SubmissionRecord {
        try await updateSubmissionStatus(submissionID: submissionID, endpoint: "publish")
    }

    func archiveSubmission(submissionID: UUID) async throws -> SubmissionRecord {
        try await updateSubmissionStatus(submissionID: submissionID, endpoint: "archive")
    }

    func updateSubmissionReviewMetadata(submissionID: UUID, nominalCode: String, isAutoCategorised: Bool) async throws -> SubmissionRecord {
        guard let baseURL = configuration.apiBaseURL else {
            guard let index = mockStorage.submissions.firstIndex(where: { $0.id == submissionID }) else {
                throw URLError(.fileDoesNotExist)
            }

            let current = mockStorage.submissions[index]
            let updated = SubmissionRecord(
                id: current.id,
                workspaceID: current.workspaceID,
                userID: current.userID,
                source: current.source,
                originalFilename: current.originalFilename,
                fileURL: current.fileURL,
                emailFrom: current.emailFrom,
                emailSubject: current.emailSubject,
                status: current.status,
                xeroStatus: current.xeroStatus,
                nominalCode: nominalCode,
                isAutoCategorised: isAutoCategorised,
                archivedAt: current.archivedAt,
                createdAt: current.createdAt,
                extractedData: current.extractedData,
                errorMessage: current.errorMessage
            )
            mockStorage.submissions[index] = updated
            return updated
        }

        var request = try makeRequest(
            url: baseURL.appending(path: "/api/v1/submissions/\(submissionID.uuidString)/review-metadata"),
            method: "PATCH"
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(
            ReviewMetadataPayload(
                nominalCode: nominalCode,
                isAutoCategorised: isAutoCategorised
            )
        )

        return try await perform(request, decode: SubmissionRecord.self)
    }

    func updateWorkspaceInboundEmail(workspaceID: UUID, email: String) async throws -> Workspace {
        guard let baseURL = configuration.apiBaseURL else {
            let updatedWorkspace = Workspace(
                id: mockStorage.workspace.id,
                companyName: mockStorage.workspace.companyName,
                jentryInboundEmail: email,
                xeroTenantID: mockStorage.workspace.xeroTenantID,
                xeroConnectionStatus: mockStorage.workspace.xeroConnectionStatus
            )
            mockStorage = DashboardPayload(
                workspace: updatedWorkspace,
                submissions: mockStorage.submissions
            )
            return updatedWorkspace
        }

        var request = try makeRequest(
            url: baseURL.appending(path: "/api/v1/workspaces/\(workspaceID.uuidString)"),
            method: "PATCH"
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(["jentryInboundEmail": email])
        return try await perform(request, decode: Workspace.self)
    }

    private func updateSubmissionStatus(submissionID: UUID, endpoint: String) async throws -> SubmissionRecord {
        guard let baseURL = configuration.apiBaseURL else {
            guard let index = mockStorage.submissions.firstIndex(where: { $0.id == submissionID }) else {
                throw URLError(.fileDoesNotExist)
            }
            let current = mockStorage.submissions[index]
            let nextStatus: DocumentStatus
            let nextXeroStatus: String?
            let archivedAt: Date?

            switch endpoint {
            case "mark-ready":
                nextStatus = .readyForXero
                nextXeroStatus = "ready_for_xero"
                archivedAt = current.archivedAt
            case "publish":
                nextStatus = .exported
                nextXeroStatus = "exported"
                archivedAt = .now
            case "archive":
                nextStatus = current.status
                nextXeroStatus = current.xeroStatus
                archivedAt = .now
            default:
                nextStatus = .queued
                nextXeroStatus = "queued"
                archivedAt = current.archivedAt
            }
            let updated = SubmissionRecord(
                id: current.id,
                workspaceID: current.workspaceID,
                userID: current.userID,
                source: current.source,
                originalFilename: current.originalFilename,
                fileURL: current.fileURL,
                emailFrom: current.emailFrom,
                emailSubject: current.emailSubject,
                status: nextStatus,
                xeroStatus: nextXeroStatus,
                nominalCode: current.nominalCode,
                isAutoCategorised: current.isAutoCategorised,
                archivedAt: archivedAt,
                createdAt: current.createdAt,
                extractedData: current.extractedData,
                errorMessage: nil
            )
            mockStorage.submissions[index] = updated
            return updated
        }

        let request = try makeRequest(
            url: baseURL.appending(path: "/api/v1/submissions/\(submissionID.uuidString)/\(endpoint)"),
            method: "POST"
        )
        return try await perform(request, decode: SubmissionRecord.self)
    }

    private func makeRequest(url: URL, method: String = "GET") throws -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method

        if let token = configuration.apiBearerToken {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        return request
    }

    private func perform<Response: Decodable>(_ request: URLRequest, decode type: Response.Type) async throws -> Response {
        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw JentryCloudError.invalidResponse
        }

        if httpResponse.statusCode == 401 || httpResponse.statusCode == 403 {
            throw JentryCloudError.authenticationRequired
        }

        return try JSONDecoder.jentry.decode(Response.self, from: data)
    }
}

nonisolated private struct ReviewMetadataPayload: Codable {
    let nominalCode: String
    let isAutoCategorised: Bool
}

nonisolated struct MultipartFormDataBuilder {
    let boundary: String
    private var body = Data()

    init(boundary: String) {
        self.boundary = boundary
    }

    func addTextField(named name: String, value: String) -> MultipartFormDataBuilder {
        var copy = self
        copy.body.append("--\(boundary)\r\n".data(using: .utf8)!)
        copy.body.append("Content-Disposition: form-data; name=\"\(name)\"\r\n\r\n".data(using: .utf8)!)
        copy.body.append("\(value)\r\n".data(using: .utf8)!)
        return copy
    }

    func addFileField(named name: String, filename: String, mimeType: String, data: Data) -> MultipartFormDataBuilder {
        var copy = self
        copy.body.append("--\(boundary)\r\n".data(using: .utf8)!)
        copy.body.append("Content-Disposition: form-data; name=\"\(name)\"; filename=\"\(filename)\"\r\n".data(using: .utf8)!)
        copy.body.append("Content-Type: \(mimeType)\r\n\r\n".data(using: .utf8)!)
        copy.body.append(data)
        copy.body.append("\r\n".data(using: .utf8)!)
        return copy
    }

    func build() -> Data {
        var copy = body
        copy.append("--\(boundary)--\r\n".data(using: .utf8)!)
        return copy
    }
}

nonisolated enum InboundEmailAliasGenerator {
    static func makeAlias(companyName: String, existingAliases: Set<String>) -> String {
        let stripped = companyName
            .lowercased()
            .replacingOccurrences(of: "\\b(ltd|limited)\\b", with: "", options: .regularExpression)
            .replacingOccurrences(of: "[^a-z0-9]+", with: "", options: .regularExpression)

        let base = stripped.isEmpty ? "workspace" : stripped
        var candidate = base
        var suffix = 2

        while existingAliases.contains(candidate) {
            candidate = "\(base)\(suffix)"
            suffix += 1
        }

        return candidate
    }
}

private enum MockDashboardData {
    nonisolated static let workspaceID = UUID(uuidString: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE") ?? UUID()

    nonisolated static let sample = DashboardPayload(
        workspace: Workspace(
            id: workspaceID,
            companyName: "Acme Property Services",
            jentryInboundEmail: "acme@inbound.jentry.co.uk",
            xeroTenantID: "demo-tenant-001",
            xeroConnectionStatus: .connected
        ),
        submissions: [
            SubmissionRecord(
                id: UUID(),
                workspaceID: workspaceID,
                userID: nil,
                source: .inboundEmail,
                originalFilename: "supplier-invoice-7841.pdf",
                fileURL: nil,
                emailFrom: "ap@supplier.co.uk",
                emailSubject: "Invoice 7841",
                status: .needsReview,
                xeroStatus: "pending_review",
                nominalCode: "310 - Cost of Goods Sold",
                isAutoCategorised: true,
                archivedAt: nil,
                createdAt: .now.addingTimeInterval(-1800),
                extractedData: ExtractedDocumentData(
                    supplierName: "Supplier Ltd",
                    invoiceNumber: "7841",
                    invoiceDate: "2026-04-22",
                    dueDate: "2026-05-22",
                    netAmount: 120,
                    vatAmount: 24,
                    grossAmount: 144,
                    currency: "GBP",
                    category: "Office supplies",
                    confidence: 0.71
                ),
                errorMessage: "Confidence below threshold."
            ),
            SubmissionRecord(
                id: UUID(),
                workspaceID: workspaceID,
                userID: nil,
                source: .appUpload,
                originalFilename: "march-travel.pdf",
                fileURL: nil,
                emailFrom: nil,
                emailSubject: nil,
                status: .exported,
                xeroStatus: "exported",
                nominalCode: "400 - Travel & Mileage",
                isAutoCategorised: false,
                archivedAt: .now.addingTimeInterval(-3600),
                createdAt: .now.addingTimeInterval(-7200),
                extractedData: nil,
                errorMessage: nil
            )
        ]
    )
}

private extension JSONDecoder {
    nonisolated static var jentry: JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }
}
