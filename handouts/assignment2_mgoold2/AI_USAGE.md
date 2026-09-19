# AI Assistance Log

Complete this file only if AI assistance was used for submitted code,
debugging, analysis, writing, or testing.

## 0. AI tools used

List every AI tool/model used, even when you rejected its output.

| Tool/model | Purpose | Submitted file or section affected |
|---|---|---|
| Claude Opus 5 | Explanation, debugging. | All the code blocks. |
| Claude Opus 5 | Explanation of concepts, validating my understanding. | All the follow-up questions. |

If you used AI for multiple substantially different tasks, repeat Sections 1
through 5 for each task or add clearly labeled entries under each section.

## 1. Tool and strategic purpose

- Tool/model: Claude Opus 5
- Task: explaining, debugging the code blocks.
- Why AI was appropriate at this point: I found that after working for a month and a half to move my father-in-law into assisted living, my mind was almost entirely scrubbed of what I'd learned in class up till the end of July.  I can tell you all about nursing home cost structures though.
- What I already understood before using it: until the end of July I'd put together an understanding of kafka that I documented here: https://github.com/mgoold/msds-682-su2026/blob/main/docs/kafka_notes.md .  I think it is not bad command of the subject up to that point in the class.  
- How it improved efficiency without replacing my understanding: basically it allowed me to complete the code blocks by asking questions, and to efficiently cannibalize the demo content.  I don't think I could have done it on time without it; I would have had to effectively re-take the whole class up to that point on the fly otherwise.

## 2. Prompt or request

Include the prompt or a faithful concise summary. Do not include credentials,
private data, or secret-bearing logs.

* Basically I started by asking Claude to map the code blocks to the corresponding demo code with line references.  
* Then as I went block by block, I copied over the demo code, and I asked some conceptual questions about what the code was doing, and then made my best effort to write it out.
* Once I'd written my own code, I went through a QA conversation with Claude until the block passed error free.  I found that my python knowledge had eroded badly from the start of the year, maybe more than my knowledge of kafka, which was depressing.

## 3. Output and engineering judgment

- Output summary:
- Suggestions accepted and why: many of the accepted suggestions I would characterize as correcting bad python code, rather than kafka knowledge.  
- Suggestions rejected and why: I didn't really have bad suggestions to reject.  Once I caught it out in an error about the number of arguments to return.
- Claude itself recounts several bad instructions it gave that I'd forgotten:
"
* I flagged your BOOTSTRAP_SERVERS value as a copy-paste error because it matched your dead cluster's host. A connectivity probe disproved it — Basic clusters are multi-tenant and share a pkc- endpoint, so an identical hostname was expected.
* I described a Stream Governance package dialog during environment creation; your console had no such step, and Schema Registry activated when the cluster was created.
* I said you had no environment; the breadcrumb in your own screenshot showed default existed.
"
- Changes I made myself: mostly enhancements or rephrases of its responses to the open ended questions at the end of the lesson.

## 4. Independent accuracy verification

Describe the tests, Confluent evidence, logs, primary documentation, manual reasoning, or comparison used to determine whether the response was correct.
Answer: 
* These are the things I did that involved some independent accuracy verification:
  * Ran python -m pytest -q against a test suite you didn't write; it went from 1 failure to 11 passing, and it caught a real bug (the TripAcceptedResponse built from the wrong fields).
  * Ran the seeder and all three consumer phases against real Confluent Cloud and read the reports rather than trusting that the code "should" work.
  * Confirmed the first and resume sequence sets were disjoint and covered 0-11, and that the base group's committed offsets were unchanged after replay.
  * Used claude + visual review for repeated code checks until QA passed cleanly. 
  
## 5. Failure recovery and fallback

If AI was wrong, repetitive, or unable to solve the problem, explain how you
recognized that and what you changed. Examples include narrowing the prompt,
adding relevant context, running a focused test, consulting primary
documentation, debugging manually, or switching to a non-AI method.

If no failure occurred, state:
- warning signs that would make you stop trusting the current answer, and
- the non-AI fallback you would use.

Answer: I do have this problem, but not on this project.  For the much larger repo for the class project, having Claude contradict its earlier findings is a constant hassle.  The fall back I have used is to continue to try and optimize how I used Claude:
* I have added Claude.md files in sub directories
* I've used using hooks and skills as warranted.
It's an ongoing struggle.

## 6. Responsibility statement

I understand and can explain the submitted consumer, Avro/Pydantic validation,
offset commits, resume/replay behavior, evidence, and conclusions. I verified
the work and remain responsible for its accuracy.
Answer: I can explain these things conceptually.  I believe that I could narrate through the code blocks I made successfully.  I'd have to prep more to go beyond that at this moment.

Name: Mark Goold

Date: 9/18/2026
