//
//  JentryTests.swift
//  JentryTests
//
//  Created by Jay Wilson on 24/04/2026.
//

import Foundation
import Testing
@testable import Jentry

struct JentryTests {
    @Test func serviceConfigurationStoresBearerToken() {
        let configuration = JentryServiceConfiguration(
            apiBaseURL: URL(string: "https://example.com"),
            apiBearerToken: "secret-token"
        )

        #expect(configuration.apiBaseURL?.absoluteString == "https://example.com")
        #expect(configuration.apiBearerToken == "secret-token")
    }

    @Test func aliasGeneratorStripsNoiseAndEnsuresUniqueness() {
        let alias = InboundEmailAliasGenerator.makeAlias(
            companyName: "Acme Limited",
            existingAliases: ["acme", "acme2"]
        )

        #expect(alias == "acme3")
    }

    @Test func reviewStatusesAreFlaggedCorrectly() {
        #expect(DocumentStatus.needsReview.requiresReview)
        #expect(DocumentStatus.failed.requiresReview)
        #expect(DocumentStatus.exported.requiresReview == false)
    }

    @Test func mockServiceTransitionsReviewDocumentToReadyForXero() async throws {
        let service = LiveJentryCloudService(configuration: JentryServiceConfiguration(bundle: .main))
        let dashboard = try await service.fetchDashboard()
        let reviewDocument = try #require(dashboard.submissions.first(where: { $0.status == .needsReview }))

        let updated = try await service.markSubmissionReadyForXero(submissionID: reviewDocument.id)

        #expect(updated.status == .readyForXero)
        #expect(updated.errorMessage == nil)
    }
}
