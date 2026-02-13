import Link from "next/link";

export default function HomePage() {
  return (
    <div className="page">
      <header className="topbar">
        <Link className="logo" href="/" aria-label="IndexQube home">
          <span className="logo-mark">IQ</span>
          IndexQube
        </Link>
        <nav className="nav">
          <Link href="#platform">Platform</Link>
          <Link href="#opportunity">Opportunity</Link>
          <Link href="#roadmap">Roadmap</Link>
          <Link href="/dashboard">Dashboard</Link>
          <Link href="#founder">Founder</Link>
          <Link href="#contact">Contact</Link>
        </nav>
      </header>

      <main>
        <section className="hero" id="platform">
          <div className="hero-copy">
            <p className="eyebrow">IndexQube platform</p>
            <h1>Index infrastructure that keeps every data source honest.</h1>
            <p className="lead">
              IndexQube is a financial technology platform for factor-based,
              volatility control, and defined outcome index calculation. We
              validate multiple providers, automate quality controls, and give
              you audit-ready lineage from ingestion to publication.
            </p>
            <div className="cta-row">
              <Link className="button primary" href="#contact">
                Request a demo
              </Link>
              <Link className="button secondary" href="#solution">
                See how it works
              </Link>
            </div>
            <div className="hero-notes">
              <span>Multi-provider validation</span>
              <span>Methodology versioning</span>
              <span>Provider migration workflows</span>
            </div>
          </div>
          <div className="hero-panel">
            <div className="terminal">
              <div className="terminal-bar">
                <span />
                <span />
                <span />
                <p>indexqube run</p>
              </div>
              <div className="terminal-body">
                <p>$ indexqube validate --index vol-control</p>
                <p>Detecting providers... bloomberg, refinitiv</p>
                <p>Running 38 quality checks...</p>
                <p>QC summary: 2 exceptions, 0 critical</p>
                <p>Publishing index series... ready</p>
              </div>
            </div>
            <div className="stat-grid">
              <div className="stat">
                <strong>Real-time parity</strong>
                <span>Monitor provider deltas before they hit production.</span>
              </div>
              <div className="stat">
                <strong>Audit trails</strong>
                <span>Every calculation step is stored and replayable.</span>
              </div>
              <div className="stat">
                <strong>Always-on</strong>
                <span>Swap providers without downtime or data loss.</span>
              </div>
            </div>
          </div>
        </section>

        <section className="split" id="opportunity">
          <div className="split-card">
            <h2>The opportunity</h2>
            <p>
              Index programs are expanding fast, but operational overhead is
              not. Teams need scale, transparency, and clean provider migrations
              without rebuilding core infrastructure.
            </p>
            <div className="pill-grid">
              <span>Index demand is growing</span>
              <span>Data quality expectations rising</span>
              <span>Provider swaps are inevitable</span>
            </div>
          </div>
          <div className="split-card">
            <h2>The problem</h2>
            <ul>
              <li>Weeks lost reconciling provider data</li>
              <li>Manual controls that do not scale</li>
              <li>Hidden risk during provider migrations</li>
              <li>Limited transparency for oversight teams</li>
            </ul>
          </div>
        </section>

        <section className="section" id="solution">
          <div className="section-head">
            <h2>Our solution</h2>
            <p>
              One platform to design, validate, and publish custom indices with
              multi-provider confidence built in.
            </p>
          </div>
          <div className="feature-grid">
            <article>
              <h3>Index design studio</h3>
              <p>
                Model methodologies, version rules, and keep approvals and
                governance in one place.
              </p>
            </article>
            <article>
              <h3>Quality control engine</h3>
              <p>
                Automated parity checks, anomaly detection, and exception
                workflows across every provider.
              </p>
            </article>
            <article>
              <h3>Production publishing</h3>
              <p>
                Schedule runs, distribute outputs, and generate client-ready
                evidence packs.
              </p>
            </article>
          </div>
        </section>

        <section className="section" id="capabilities">
          <div className="section-head">
            <h2>Enterprise-grade capabilities</h2>
            <p>
              Infrastructure that stays reliable through market stress, audits,
              and provider transitions.
            </p>
          </div>
          <div className="cap-grid">
            <div className="cap">
              <h4>Multi-provider ingestion</h4>
              <p>Automated normalization for prices, corp actions, and metadata.</p>
            </div>
            <div className="cap">
              <h4>Controlled methodology changes</h4>
              <p>Versioned releases with approval and impact reporting.</p>
            </div>
            <div className="cap">
              <h4>Resilient publishing</h4>
              <p>Staged release pipelines with confidence scoring.</p>
            </div>
            <div className="cap">
              <h4>Provider migration suite</h4>
              <p>Parallel runs, parity dashboards, and rollback plans.</p>
            </div>
            <div className="cap">
              <h4>Audit-ready reporting</h4>
              <p>Lineage logs and evidence bundles for internal oversight.</p>
            </div>
            <div className="cap">
              <h4>Risk-based QC</h4>
              <p>Thresholds, controls, and alerts tuned by index type.</p>
            </div>
          </div>
        </section>

        <section className="section" id="roadmap">
          <div className="section-head">
            <h2>Roadmap</h2>
            <p>Where IndexQube is headed in 2026.</p>
          </div>
          <div className="roadmap">
            <div className="roadmap-item">
              <p className="quarter">Q1 2026</p>
              <h3>Private beta</h3>
              <p>
                Core index engine, QC rules, provider comparison dashboards.
              </p>
            </div>
            <div className="roadmap-item">
              <p className="quarter">Q2 2026</p>
              <h3>Program launch</h3>
              <p>
                Workflow approvals, audit packs, and curated data pipelines.
              </p>
            </div>
            <div className="roadmap-item">
              <p className="quarter">Q3 2026</p>
              <h3>Enterprise rollout</h3>
              <p>Migration suite, SLA monitoring, expanded provider catalog.</p>
            </div>
            <div className="roadmap-item">
              <p className="quarter">Q4 2026</p>
              <h3>Scale</h3>
              <p>Regional redundancy, multi-cloud readiness, partner APIs.</p>
            </div>
          </div>
        </section>

        <section className="section" id="founder">
          <div className="founder">
            <div className="founder-card">
              <div className="avatar">R</div>
              <div>
                <h2>Hi, I am Revanth.</h2>
                <p>
                  I have spent years building index workflows and data pipelines.
                  IndexQube is the infrastructure I wanted when I had to
                  reconcile multiple providers under tight deadlines.
                </p>
                <div className="founder-tags">
                  <span>Financial data systems</span>
                  <span>Index methodology design</span>
                  <span>Enterprise delivery</span>
                </div>
              </div>
            </div>
            <div className="founder-panel">
              <h3>What we believe</h3>
              <ul>
                <li>Data quality is an engineering discipline.</li>
                <li>Provider migration should be routine, not risky.</li>
                <li>Every calculation must be explainable on demand.</li>
              </ul>
            </div>
          </div>
        </section>

        <section className="cta" id="contact">
          <div>
            <h2>Ready to design resilient indices?</h2>
            <p>Join the design partner program for 2026.</p>
          </div>
          <div className="cta-row">
            <a className="button primary" href="mailto:hello@indexqube.com">
              Request early access
            </a>
            <a
              className="button secondary"
              href="https://www.linkedin.com"
              target="_blank"
              rel="noopener noreferrer"
            >
              LinkedIn
            </a>
          </div>
        </section>
      </main>

      <footer className="footer">
        <p>IndexQube. Financial index infrastructure built for resilience.</p>
        <p>Based in NYC. Serving global asset managers and index providers.</p>
      </footer>
    </div>
  );
}
