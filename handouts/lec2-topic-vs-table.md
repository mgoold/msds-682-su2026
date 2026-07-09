<section class="concept-slide" aria-label="Kafka topic and database table comparison">
  <header class="concept-slide-header">
    <p class="eyebrow">Lec 2 reference</p>
    <h2>Kafka Topic vs Database Table</h2>
    <p>A table describes records. A topic carries time-ordered event messages.</p>
  </header>

  <div class="concept-compare">
    <article class="concept-panel table-panel">
      <h3>Database Table</h3>
      <div class="mini-data-table" role="table" aria-label="Example orders table">
        <div class="table-row table-head" role="row">
          <span role="columnheader">order_id</span>
          <span role="columnheader">user_id</span>
          <span role="columnheader">amount</span>
          <span role="columnheader">status</span>
        </div>
        <div class="table-row" role="row">
          <span role="cell">1001</span>
          <span role="cell">Alice</span>
          <span role="cell">$24.50</span>
          <span role="cell">paid</span>
        </div>
        <div class="table-row" role="row">
          <span role="cell">1002</span>
          <span role="cell">Bob</span>
          <span role="cell">$18.00</span>
          <span role="cell">pending</span>
        </div>
      </div>
      <p class="concept-note"><strong>Think:</strong> rows with fields that can be queried and updated.</p>
    </article>

    <article class="concept-panel topic-panel">
      <h3>Kafka Topic</h3>
      <div class="partition-card">
        <strong>orders.events.v1 / partition 0</strong>
        <div class="event-row">
          <span>offset 0</span>
          <span>key=Alice | value=order_created</span>
          <span>10:01</span>
        </div>
        <div class="event-row">
          <span>offset 1</span>
          <span>key=Alice | value=order_paid</span>
          <span>10:03</span>
        </div>
      </div>
      <div class="partition-card">
        <strong>orders.events.v1 / partition 1</strong>
        <div class="event-row">
          <span>offset 0</span>
          <span>key=Bob | value=order_created</span>
          <span>10:02</span>
        </div>
      </div>
      <div class="topic-parts">
        <span><strong>Message</strong> event record</span>
        <span><strong>Key</strong> partition identity</span>
        <span><strong>Value</strong> payload</span>
        <span><strong>Timestamp</strong> event/write time</span>
      </div>
    </article>
  </div>

  <div class="concept-takeaways" aria-label="Key differences">
    <article>
      <h3>Unit</h3>
      <p>Table stores rows. Topic stores messages or events.</p>
    </article>
    <article>
      <h3>Change</h3>
      <p>Rows can update. Topic messages are immutable; changes arrive as new messages.</p>
    </article>
    <article>
      <h3>Order</h3>
      <p>Topic is temporal. Order is preserved within one partition.</p>
    </article>
    <article>
      <h3>Read Pattern</h3>
      <p>Database queries records. Consumers read events from offsets over time.</p>
    </article>
  </div>
</section>
