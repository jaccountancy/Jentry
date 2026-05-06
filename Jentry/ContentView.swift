//
//  ContentView.swift
//  Jentry
//
//  Created by Jay Wilson on 24/04/2026.
//

import SwiftUI
import PhotosUI
import UniformTypeIdentifiers
import Combine

struct ContentView: View {
    @StateObject private var viewModel = DashboardViewModel()
    @State private var isShowingFileImporter = false
    @State private var selectedPhotoItem: PhotosPickerItem?
    @State private var selectedSubmission: SubmissionRecord?
    @State private var isShowingError = false
    @State private var selectedDocumentScreen: DocumentScreen = .inbox
    private let refreshInterval: Duration = .seconds(30)

    var body: some View {
        NavigationStack {
            List {
                dashboardSummarySection
                workspaceSection
                uploadSection
                pipelineSection
                submissionsSection
                architectureSection
            }
            .listStyle(.insetGrouped)
            .navigationTitle("Jentry")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        Task { await viewModel.refresh() }
                    } label: {
                        if viewModel.isRefreshing {
                            ProgressView()
                        } else {
                            Image(systemName: "arrow.clockwise")
                        }
                    }
                    .disabled(viewModel.isRefreshing)
                }
            }
            .task {
                await viewModel.refresh()
            }
            .task {
                await pollForUpdates()
            }
            .refreshable {
                await viewModel.refresh()
            }
            .fileImporter(
                isPresented: $isShowingFileImporter,
                allowedContentTypes: [.pdf, .image],
                allowsMultipleSelection: false
            ) { result in
                handleFileImport(result)
            }
            .task(id: selectedPhotoItem) {
                await handleSelectedPhotoItem()
            }
            .sheet(item: $selectedSubmission) { submission in
                ReviewSubmissionView(
                    submission: submission,
                    onRetry: {
                        Task { await viewModel.retry(submissionID: submission.id) }
                    },
                    onMarkReady: {
                        Task { await viewModel.markReadyForXero(submissionID: submission.id) }
                    },
                    onPublish: {
                        Task { await viewModel.publish(submissionID: submission.id) }
                    },
                    onArchive: {
                        Task { await viewModel.archive(submissionID: submission.id) }
                    }
                )
            }
            .onChange(of: viewModel.errorMessage) { _, newValue in
                isShowingError = newValue != nil
            }
            .alert("Upload Error", isPresented: $isShowingError) {
                Button("OK") { viewModel.errorMessage = nil }
            } message: {
                Text(viewModel.errorMessage ?? "")
            }
        }
    }

    private var dashboardSummarySection: some View {
        Section {
            VStack(alignment: .leading, spacing: 12) {
                Text("Cloud-first document operations")
                    .font(.title3.weight(.semibold))

                Text("Uploads and inbound email go to the backend. Extraction, validation, queueing, and Xero export run server-side whether the app is open or not.")
                    .foregroundStyle(.secondary)

                if viewModel.usesMockBackend {
                    Label("Preview mode: no API base URL configured.", systemImage: "server.rack")
                        .font(.footnote)
                        .foregroundStyle(.orange)
                }
            }
            .padding(.vertical, 4)
        }
    }

    private var workspaceSection: some View {
        Section("Workspace") {
            if let workspace = viewModel.workspace {
                VStack(alignment: .leading, spacing: 12) {
                    Text(workspace.companyName)
                        .font(.headline)

                    LabeledContent("Inbound email", value: workspace.jentryInboundEmail)
                    LabeledContent("Xero tenant", value: workspace.xeroTenantID ?? "Not connected")
                    LabeledContent("Connection", value: workspace.xeroConnectionStatus.displayName)

                    Text("Forward supplier invoices to this address to create submissions without opening the app.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
                .padding(.vertical, 4)
            } else {
                ContentUnavailableView("No Workspace", systemImage: "building.2", description: Text("Refresh after the backend is configured."))
            }
        }
    }

    private var uploadSection: some View {
        Section("Submit Documents") {
            VStack(alignment: .leading, spacing: 12) {
                Text("The app only uploads files to the backend. OCR and Xero sync do not run on-device.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)

                Button("Upload PDF or Image") {
                    isShowingFileImporter = true
                }
                .buttonStyle(.borderedProminent)

                PhotosPicker(selection: $selectedPhotoItem, matching: .images) {
                    Label("Choose From Photos", systemImage: "photo.on.rectangle")
                }

                if viewModel.isUploading {
                    ProgressView("Uploading to Jentry Cloud…")
                }
            }
            .padding(.vertical, 4)
        }
    }

    private var pipelineSection: some View {
        Section("Pipeline Status") {
            HStack {
                Label("Inbox", systemImage: "tray.full")
                Spacer()
                Text("\(viewModel.inboxSubmissions.count)")
                    .foregroundStyle(.secondary)
            }

            HStack {
                Label("Archive", systemImage: "archivebox")
                Spacer()
                Text("\(viewModel.archivedSubmissions.count)")
                    .foregroundStyle(.secondary)
            }

            ForEach(DocumentStatus.dashboardOrder, id: \.self) { status in
                HStack {
                    Label(status.displayName, systemImage: status.symbolName)
                        .foregroundStyle(statusColor(status))
                    Spacer()
                    Text("\(viewModel.submissions.filter { $0.status == status }.count)")
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    private var submissionsSection: some View {
        Section {
            Picker("Document Screen", selection: $selectedDocumentScreen) {
                ForEach(DocumentScreen.allCases) { screen in
                    Text(screen.title).tag(screen)
                }
            }
            .pickerStyle(.segmented)

            if activeSubmissions.isEmpty {
                ContentUnavailableView(
                    selectedDocumentScreen.emptyTitle,
                    systemImage: selectedDocumentScreen.systemImage,
                    description: Text(selectedDocumentScreen.emptyDescription)
                )
            } else {
                ForEach(activeSubmissions) { submission in
                    Button {
                        selectedSubmission = submission
                    } label: {
                        SubmissionRow(submission: submission)
                    }
                    .buttonStyle(.plain)
                }
            }
        } header: {
            Text(selectedDocumentScreen.title)
        } footer: {
            Text(selectedDocumentScreen.footerText)
        }
    }

    private var activeSubmissions: [SubmissionRecord] {
        switch selectedDocumentScreen {
        case .inbox:
            return viewModel.inboxSubmissions
        case .archive:
            return viewModel.archivedSubmissions
        }
    }

    private var architectureSection: some View {
        Section("Architecture") {
            VStack(alignment: .leading, spacing: 8) {
                Text("App upload / inbound email -> Railway API -> PostgreSQL queue -> background worker -> OCR/validation -> Xero")
                Text("The app acts as dashboard, upload surface, and review screen for low-confidence or failed documents.")
                    .foregroundStyle(.secondary)
                    .font(.footnote)
            }
            .padding(.vertical, 4)
        }
    }

    private func handleFileImport(_ result: Result<[URL], Error>) {
        guard case let .success(urls) = result, let url = urls.first else {
            if case let .failure(error) = result {
                viewModel.errorMessage = error.localizedDescription
            }
            return
        }

        Task {
            do {
                let payload = try DocumentUploadPayload.fileURL(url)
                await viewModel.upload(payload)
            } catch {
                viewModel.errorMessage = error.localizedDescription
            }
        }
    }

    private func handleSelectedPhotoItem() async {
        guard let selectedPhotoItem else { return }
        defer { self.selectedPhotoItem = nil }

        do {
            guard let data = try await selectedPhotoItem.loadTransferable(type: Data.self) else {
                viewModel.errorMessage = "The selected photo could not be loaded."
                return
            }
            let payload = DocumentUploadPayload(
                filename: "photo-upload.jpg",
                mimeType: "image/jpeg",
                data: data,
                source: .appUpload
            )
            await viewModel.upload(payload)
        } catch {
            viewModel.errorMessage = error.localizedDescription
        }
    }

    private func pollForUpdates() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: refreshInterval)
            guard Task.isCancelled == false else { return }
            await viewModel.refresh()
        }
    }

    private func statusColor(_ status: DocumentStatus) -> Color {
        switch status {
        case .received, .queued:
            return .blue
        case .processing, .exporting:
            return .orange
        case .needsReview:
            return .yellow
        case .readyForXero, .exported:
            return .green
        case .failed:
            return .red
        }
    }
}

private enum DocumentScreen: String, CaseIterable, Identifiable {
    case inbox
    case archive

    var id: String { rawValue }

    var title: String {
        switch self {
        case .inbox:
            return "Inbox"
        case .archive:
            return "Archive"
        }
    }

    var systemImage: String {
        switch self {
        case .inbox:
            return "tray.full"
        case .archive:
            return "archivebox"
        }
    }

    var emptyTitle: String {
        switch self {
        case .inbox:
            return "Inbox Clear"
        case .archive:
            return "No Archived Documents"
        }
    }

    var emptyDescription: String {
        switch self {
        case .inbox:
            return "New uploads, processing items, review items, and failures will appear here."
        case .archive:
            return "Published and completed documents will move here."
        }
    }

    var footerText: String {
        switch self {
        case .inbox:
            return "Keep Inbox clear by working through new, processing, review, and failed documents."
        case .archive:
            return "Archive holds completed documents that can be set aside once processed."
        }
    }
}

@MainActor
final class DashboardViewModel: ObservableObject {
    @Published var workspace: Workspace?
    @Published var submissions: [SubmissionRecord] = []
    @Published var isRefreshing = false
    @Published var isUploading = false
    @Published var errorMessage: String?

    let usesMockBackend: Bool
    private let service: JentryCloudServiceProtocol

    init(service: JentryCloudServiceProtocol? = nil) {
        let liveService = LiveJentryCloudService()
        self.service = service ?? liveService
        self.usesMockBackend = liveService.configuration.apiBaseURL == nil
    }

    var inboxSubmissions: [SubmissionRecord] {
        submissions.filter(\.isInbox)
    }

    var archivedSubmissions: [SubmissionRecord] {
        submissions.filter(\.isArchived)
    }

    func refresh() async {
        isRefreshing = true
        defer { isRefreshing = false }

        do {
            let dashboard = try await service.fetchDashboard()
            workspace = dashboard.workspace
            submissions = dashboard.submissions
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func upload(_ payload: DocumentUploadPayload) async {
        isUploading = true
        defer { isUploading = false }

        do {
            let submission = try await service.uploadDocument(payload)
            submissions.insert(submission, at: 0)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func retry(submissionID: UUID) async {
        do {
            let updatedSubmission = try await service.retrySubmission(submissionID: submissionID)
            replace(updatedSubmission)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func markReadyForXero(submissionID: UUID) async {
        do {
            let updatedSubmission = try await service.markSubmissionReadyForXero(submissionID: submissionID)
            replace(updatedSubmission)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func publish(submissionID: UUID) async {
        do {
            let updatedSubmission = try await service.publishSubmission(submissionID: submissionID)
            replace(updatedSubmission)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func archive(submissionID: UUID) async {
        do {
            let updatedSubmission = try await service.archiveSubmission(submissionID: submissionID)
            replace(updatedSubmission)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func replace(_ updatedSubmission: SubmissionRecord) {
        guard let index = submissions.firstIndex(where: { $0.id == updatedSubmission.id }) else { return }
        submissions[index] = updatedSubmission
    }
}

private struct SubmissionRow: View {
    let submission: SubmissionRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(submission.originalFilename)
                    .font(.headline)
                    .foregroundStyle(.primary)
                Spacer()
                StatusBadge(status: submission.status)
            }

            HStack {
                Text(submission.source.displayName)
                Spacer()
                Text(submission.createdAt.formatted(date: .abbreviated, time: .shortened))
            }
            .font(.footnote)
            .foregroundStyle(.secondary)

            if let subject = submission.emailSubject {
                Text(subject)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.vertical, 6)
    }
}

private struct StatusBadge: View {
    let status: DocumentStatus

    var body: some View {
        Label(status.displayName, systemImage: status.symbolName)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(tint.opacity(0.12), in: Capsule())
            .foregroundStyle(tint)
    }

    private var tint: Color {
        switch status {
        case .received, .queued:
            return .blue
        case .processing, .exporting:
            return .orange
        case .needsReview:
            return .yellow
        case .readyForXero, .exported:
            return .green
        case .failed:
            return .red
        }
    }
}

private struct ReviewSubmissionView: View {
    let submission: SubmissionRecord
    let onRetry: () -> Void
    let onMarkReady: () -> Void
    let onPublish: () -> Void
    let onArchive: () -> Void
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            List {
                Section("Document") {
                    LabeledContent("Filename", value: submission.originalFilename)
                    LabeledContent("Status", value: submission.status.displayName)
                    LabeledContent("Source", value: submission.source.displayName)
                    if let errorMessage = submission.errorMessage {
                        LabeledContent("Issue", value: errorMessage)
                    }
                }

                if let extractedData = submission.extractedData {
                    Section("Extracted Data") {
                        LabeledContent("Supplier", value: extractedData.supplierName)
                        LabeledContent("Invoice", value: extractedData.invoiceNumber)
                        LabeledContent("Invoice date", value: extractedData.invoiceDate)
                        LabeledContent("Due date", value: extractedData.dueDate)
                        LabeledContent("Gross", value: extractedData.grossAmount.formatted(.currency(code: extractedData.currency)))
                        LabeledContent("Confidence", value: extractedData.confidence.formatted(.percent.precision(.fractionLength(0))))
                    }
                }

                Section("Actions") {
                    Button("Retry Processing") {
                        onRetry()
                        dismiss()
                    }
                    if submission.status == .needsReview {
                        Button("Mark Ready For Xero") {
                            onMarkReady()
                            dismiss()
                        }
                    }
                    if submission.status == .readyForXero || submission.status == .exporting {
                        Button("Publish To Xero") {
                            onPublish()
                            dismiss()
                        }
                    }
                    if submission.isArchived == false && submission.status == .exported {
                        Button("Move To Archive") {
                            onArchive()
                            dismiss()
                        }
                    }
                }
            }
            .navigationTitle("Review")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}
