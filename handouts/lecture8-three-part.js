document.documentElement.classList.add("js");

const slides = [...document.querySelectorAll(".slide")];
const progress = document.querySelector(".progress");

slides.forEach((slide, index) => {
  const pager = slide.querySelector(".pager");
  if (pager) pager.textContent = `${index + 1} / ${slides.length}`;

  const crumb = slide.querySelector(".crumb");
  if (crumb) crumb.textContent = document.body.dataset.deck || "Lecture 8";

  const timing = slide.querySelector(".timing");
  if (timing) timing.textContent = slide.dataset.timing || "";
});

const observer = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) return;

    const frame = entry.target.querySelector(".frame");
    if (frame) frame.classList.add("in");

    if (entry.intersectionRatio > 0.6) {
      const index = slides.indexOf(entry.target);
      if (progress) progress.style.width = `${((index + 1) / slides.length) * 100}%`;
      if (entry.target.id) history.replaceState(null, "", `#${entry.target.id}`);
    }
  });
}, { threshold: [0.18, 0.65] });

slides.forEach((slide) => observer.observe(slide));

function currentSlideIndex() {
  const viewportCenter = window.scrollY + window.innerHeight / 2;
  return slides.reduce((bestIndex, slide, index) => {
    const best = slides[bestIndex];
    const slideCenter = slide.offsetTop + slide.offsetHeight / 2;
    const bestCenter = best.offsetTop + best.offsetHeight / 2;
    return Math.abs(slideCenter - viewportCenter) < Math.abs(bestCenter - viewportCenter)
      ? index
      : bestIndex;
  }, 0);
}

function move(delta) {
  const next = Math.max(0, Math.min(slides.length - 1, currentSlideIndex() + delta));
  slides[next].scrollIntoView({ behavior: "smooth" });
}

window.addEventListener("keydown", (event) => {
  if (["ArrowDown", "ArrowRight", "PageDown", " "].includes(event.key)) {
    event.preventDefault();
    move(1);
  } else if (["ArrowUp", "ArrowLeft", "PageUp"].includes(event.key)) {
    event.preventDefault();
    move(-1);
  } else if (event.key === "Home") {
    event.preventDefault();
    slides[0].scrollIntoView({ behavior: "smooth" });
  } else if (event.key === "End") {
    event.preventDefault();
    slides.at(-1).scrollIntoView({ behavior: "smooth" });
  }
});
