document.addEventListener("DOMContentLoaded", () => {
	const menuToggle = document.querySelector(".menu-toggle");
	const nav = document.querySelector(".main-nav");
	if (menuToggle && nav) {
		menuToggle.addEventListener("click", () => {
			nav.classList.toggle("open");
			menuToggle.classList.toggle("open");
		});
		document.querySelectorAll(".main-nav a").forEach((link) => {
			link.addEventListener("click", () => {
				nav.classList.remove("open");
				menuToggle.classList.remove("open");
			});
		});
	}

	// Smooth scroll
	document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
		anchor.addEventListener("click", function (e) {
			const target = document.querySelector(this.getAttribute("href"));
			if (target) {
				e.preventDefault();
				target.scrollIntoView({ behavior: "smooth" });
			}
		});
	});

	// Reveal on scroll
	const observer = new IntersectionObserver(
		(entries) => {
			entries.forEach((entry) => {
				if (entry.isIntersecting) {
					entry.target.classList.add("reveal");
				}
			});
		},
		{ threshold: 0.15 }
	);
	document.querySelectorAll("section").forEach((sec) => observer.observe(sec));
});
